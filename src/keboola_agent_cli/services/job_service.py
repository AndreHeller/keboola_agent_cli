"""Job listing service - business logic for listing jobs from Queue API.

Orchestrates multi-project job retrieval, filtering, and aggregation
without knowing about CLI or HTTP details.
"""

from collections.abc import Callable
from typing import Any

from ..client import KeboolaClient
from ..config_store import ConfigStore
from ..errors import ConfigError, KeboolaApiError
from ..models import ProjectConfig

ClientFactory = Callable[[str, str], KeboolaClient]


def default_client_factory(stack_url: str, token: str) -> KeboolaClient:
    """Create a KeboolaClient with the given stack URL and token."""
    return KeboolaClient(stack_url=stack_url, token=token)


class JobService:
    """Business logic for listing Keboola jobs from the Queue API.

    Supports multi-project aggregation: queries multiple projects in sequence,
    collects results, and reports per-project errors without stopping others.

    Uses dependency injection for config_store and client_factory to enable
    easy testing with mocks.
    """

    def __init__(
        self,
        config_store: ConfigStore,
        client_factory: ClientFactory | None = None,
    ) -> None:
        self._config_store = config_store
        self._client_factory = client_factory or default_client_factory

    def resolve_projects(self, aliases: list[str] | None = None) -> dict[str, ProjectConfig]:
        """Resolve project aliases to ProjectConfig instances.

        Args:
            aliases: Specific project aliases to resolve. If None or empty,
                     returns all configured projects.

        Returns:
            Dict mapping alias to ProjectConfig for the resolved projects.

        Raises:
            ConfigError: If any specified alias is not found in the config.
        """
        config = self._config_store.load()

        if not aliases:
            return dict(config.projects)

        resolved: dict[str, ProjectConfig] = {}
        for alias in aliases:
            if alias not in config.projects:
                raise ConfigError(f"Project '{alias}' not found.")
            resolved[alias] = config.projects[alias]

        return resolved

    def list_jobs(
        self,
        aliases: list[str] | None = None,
        component_id: str | None = None,
        config_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        """List jobs across one or multiple projects.

        Queries each resolved project's Queue API for jobs, aggregates
        results into a unified list. Per-project errors are collected
        but do not stop other projects from being queried.

        Args:
            aliases: Project aliases to query. None means all projects.
            component_id: Optional filter by component ID.
            config_id: Optional filter by config ID.
            status: Optional filter by job status.
            limit: Max number of jobs per project (1-500, default 50).

        Returns:
            Dict with keys:
                - "jobs": list of job dicts with project_alias added
                - "errors": list of error dicts with project_alias,
                  error_code, message

        Raises:
            ConfigError: If a specified alias is not found (before querying).
        """
        projects = self.resolve_projects(aliases)

        all_jobs: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []

        for alias, project in projects.items():
            client = self._client_factory(project.stack_url, project.token)
            try:
                jobs = client.list_jobs(
                    component_id=component_id,
                    config_id=config_id,
                    status=status,
                    limit=limit,
                )
                for job in jobs:
                    job["project_alias"] = alias
                    all_jobs.append(job)
            except KeboolaApiError as exc:
                errors.append(
                    {
                        "project_alias": alias,
                        "error_code": exc.error_code,
                        "message": exc.message,
                    }
                )
            finally:
                client.close()

        return {"jobs": all_jobs, "errors": errors}

    def get_job_detail(
        self,
        alias: str,
        job_id: str,
    ) -> dict[str, Any]:
        """Get detailed information about a specific job.

        Args:
            alias: Project alias to query.
            job_id: The job ID.

        Returns:
            Dict with the full job detail from the Queue API,
            plus a "project_alias" key.

        Raises:
            ConfigError: If the alias is not found.
            KeboolaApiError: If the API call fails.
        """
        projects = self.resolve_projects([alias])
        project = projects[alias]

        client = self._client_factory(project.stack_url, project.token)
        try:
            detail = client.get_job_detail(job_id)
        finally:
            client.close()

        detail["project_alias"] = alias
        return detail
