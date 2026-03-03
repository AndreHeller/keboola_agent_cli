"""Keboola API client with retry, timeouts, and token masking.

This is the only module that communicates with the Keboola Storage API
and the Keboola Queue API. All HTTP details, endpoint URLs, and error
mapping are encapsulated here.

Inherits shared retry/error logic from BaseHttpClient.
"""

import logging
import time
from typing import Any
from urllib.parse import quote, urlparse, urlunparse

import httpx

from . import __version__
from .constants import (
    DEFAULT_JOB_LIMIT,
    DEFAULT_TIMEOUT,
    STORAGE_JOB_MAX_WAIT,
    STORAGE_JOB_POLL_INTERVAL,
)
from .errors import KeboolaApiError
from .http_base import BaseHttpClient
from .models import TokenVerifyResponse

logger = logging.getLogger(__name__)


class KeboolaClient(BaseHttpClient):
    """HTTP client for the Keboola Storage API and Queue API.

    Provides methods to interact with Keboola endpoints with built-in
    retry logic (exponential backoff for 429/5xx), timeouts, and
    automatic token masking in error messages.

    Inherits _do_request() and _raise_api_error() from BaseHttpClient.
    """

    def __init__(self, stack_url: str, token: str) -> None:
        self._stack_url = stack_url.rstrip("/")
        headers = {
            "X-StorageApi-Token": token,
            "User-Agent": f"keboola-agent-cli/{__version__}",
        }
        super().__init__(
            base_url=self._stack_url,
            token=token,
            headers=headers,
            timeout=DEFAULT_TIMEOUT,
        )
        self._queue_client: httpx.Client | None = None

    @property
    def _queue_base_url(self) -> str:
        """Derive Queue API base URL from the Storage API URL.

        Replaces 'connection.' with 'queue.' in the hostname.
        E.g. https://connection.keboola.com -> https://queue.keboola.com
        """
        parsed = urlparse(self._stack_url)
        hostname = parsed.hostname or ""
        queue_host = hostname.replace("connection.", "queue.", 1)
        if queue_host == hostname:
            logger.warning("Queue URL derivation did not change hostname: %s", hostname)
        return urlunparse(parsed._replace(netloc=queue_host))

    def close(self) -> None:
        """Close the underlying HTTP clients."""
        super().close()
        if self._queue_client is not None:
            self._queue_client.close()

    def __enter__(self) -> "KeboolaClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Execute a Storage API request with retry."""
        return self._do_request(method, path, **kwargs)

    def _queue_request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Execute a Queue API request with retry. Lazily creates the queue client."""
        if self._queue_client is None:
            self._queue_client = httpx.Client(
                base_url=self._queue_base_url,
                timeout=DEFAULT_TIMEOUT,
                headers=self._client._headers.copy(),
            )
        return self._do_request(
            method, path,
            client=self._queue_client,
            base_url=self._queue_base_url,
            **kwargs,
        )

    def verify_token(self) -> TokenVerifyResponse:
        """Verify the storage API token and retrieve project information.

        Returns:
            TokenVerifyResponse with project name, ID, and token description.

        Raises:
            KeboolaApiError: If token is invalid (401) or other API error.
        """
        response = self._request("GET", "/v2/storage/tokens/verify")
        data = response.json()

        return TokenVerifyResponse(
            token_id=str(data.get("id", "")),
            token_description=data.get("description", ""),
            project_id=data.get("owner", {}).get("id"),
            project_name=data.get("owner", {}).get("name", ""),
            owner_name=data.get("owner", {}).get("name", ""),
        )

    def list_components(self, component_type: str | None = None) -> list[dict[str, Any]]:
        """List components with their configurations.

        Args:
            component_type: Optional filter (extractor, writer, transformation, application).

        Returns:
            List of component dicts from the API.
        """
        params: dict[str, str] = {"include": "configuration"}
        if component_type:
            params["componentType"] = component_type

        response = self._request("GET", "/v2/storage/components", params=params)
        return response.json()

    def get_config_detail(self, component_id: str, config_id: str) -> dict[str, Any]:
        """Get detailed information about a specific configuration.

        Args:
            component_id: The component ID (e.g. keboola.ex-db-snowflake).
            config_id: The configuration ID.

        Returns:
            Configuration detail dict from the API.
        """
        safe_component_id = quote(component_id, safe="")
        safe_config_id = quote(config_id, safe="")
        response = self._request(
            "GET",
            f"/v2/storage/components/{safe_component_id}/configs/{safe_config_id}",
        )
        return response.json()

    def _wait_for_storage_job(self, job: dict[str, Any]) -> dict[str, Any]:
        """Poll a Storage API job until it reaches a terminal state.

        Branch create/delete are async operations that return a job object.
        This method polls until the job completes or fails.

        Args:
            job: Initial job response from POST/DELETE.

        Returns:
            Completed job dict (with results on success).

        Raises:
            KeboolaApiError: If the job fails or times out.
        """
        job_id = job.get("id")
        if job.get("status") in ("success", "error"):
            return job

        deadline = time.monotonic() + STORAGE_JOB_MAX_WAIT
        while time.monotonic() < deadline:
            time.sleep(STORAGE_JOB_POLL_INTERVAL)
            response = self._request("GET", f"/v2/storage/jobs/{job_id}")
            job = response.json()
            status = job.get("status")
            if status == "success":
                return job
            if status == "error":
                error_msg = job.get("error", {}).get("message", "Storage job failed")
                raise KeboolaApiError(
                    message=error_msg,
                    status_code=500,
                    error_code="STORAGE_JOB_FAILED",
                    retryable=False,
                )
        raise KeboolaApiError(
            message=f"Storage job {job_id} did not complete within {STORAGE_JOB_MAX_WAIT}s",
            status_code=504,
            error_code="STORAGE_JOB_TIMEOUT",
            retryable=True,
        )

    def create_dev_branch(self, name: str, description: str = "") -> dict[str, Any]:
        """Create a new development branch (waits for async job to complete).

        The Storage API returns an async job. This method polls until the job
        completes and returns the branch data from the job results.

        Args:
            name: Branch name.
            description: Optional branch description.

        Returns:
            Branch dict with id, name, description, created, etc.

        Raises:
            KeboolaApiError: If the API call or job fails.
        """
        body: dict[str, str] = {"name": name}
        if description:
            body["description"] = description
        response = self._request("POST", "/v2/storage/dev-branches", json=body)
        job = self._wait_for_storage_job(response.json())
        return job.get("results", {})

    def delete_dev_branch(self, branch_id: int) -> None:
        """Delete a development branch (waits for async job to complete).

        Args:
            branch_id: The branch ID to delete.

        Raises:
            KeboolaApiError: If the API call or job fails.
        """
        response = self._request("DELETE", f"/v2/storage/dev-branches/{branch_id}")
        self._wait_for_storage_job(response.json())

    def list_dev_branches(self) -> list[dict[str, Any]]:
        """List development branches for the project.

        Returns:
            List of branch dicts from the API.
        """
        response = self._request("GET", "/v2/storage/dev-branches")
        return response.json()

    def list_buckets(self, include: str | None = None) -> list[dict[str, Any]]:
        """List storage buckets with optional extended information.

        Args:
            include: Optional include parameter (e.g. "linkedBuckets" for sharing info).

        Returns:
            List of bucket dicts from the API.
        """
        params: dict[str, str] = {}
        if include:
            params["include"] = include
        response = self._request("GET", "/v2/storage/buckets", params=params)
        return response.json()

    def list_jobs(
        self,
        component_id: str | None = None,
        config_id: str | None = None,
        status: str | None = None,
        limit: int = DEFAULT_JOB_LIMIT,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """List jobs from the Queue API.

        Args:
            component_id: Optional filter by component ID.
            config_id: Optional filter by config ID (requires component_id).
            status: Optional filter by job status.
            limit: Max number of jobs to return (1-500).
            offset: Offset for pagination.

        Returns:
            List of job dicts from the Queue API.
        """
        params: dict[str, str | int] = {"limit": limit, "offset": offset}
        if component_id:
            params["component"] = component_id
        if config_id:
            params["config"] = config_id
        if status:
            params["status"] = status

        response = self._queue_request("GET", "/search/jobs", params=params)
        return response.json()

    def get_job_detail(self, job_id: str) -> dict[str, Any]:
        """Get detailed information about a specific job from the Queue API.

        Args:
            job_id: The job ID.

        Returns:
            Job detail dict from the Queue API.
        """
        safe_job_id = quote(job_id, safe="")
        response = self._queue_request("GET", f"/jobs/{safe_job_id}")
        return response.json()
