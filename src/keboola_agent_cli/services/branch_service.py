"""Branch listing service - business logic for listing development branches.

Orchestrates multi-project branch retrieval in parallel, annotates with
project alias, and aggregates results.
"""

from typing import Any

from ..errors import KeboolaApiError
from ..models import ProjectConfig
from .base import BaseService


class BranchService(BaseService):
    """Business logic for listing Keboola development branches.

    Supports multi-project aggregation: queries multiple projects in parallel
    using ThreadPoolExecutor, collects results, and reports per-project errors
    without stopping others.

    Uses dependency injection for config_store and client_factory.
    """

    def _fetch_project_branches(
        self,
        alias: str,
        project: ProjectConfig,
    ) -> tuple[str, list[dict[str, Any]], bool] | tuple[str, dict[str, str]]:
        """Fetch development branches for a single project (runs in a worker thread).

        Returns either (alias, branches_list, True) on success
        or (alias, error_dict) on failure.
        """
        client = self._client_factory(project.stack_url, project.token)
        try:
            raw_branches = client.list_dev_branches()
            branches: list[dict[str, Any]] = []
            for branch in raw_branches:
                branches.append(
                    {
                        "project_alias": alias,
                        "id": branch.get("id"),
                        "name": branch.get("name", ""),
                        "isDefault": branch.get("isDefault", False),
                        "created": branch.get("created", ""),
                        "description": branch.get("description", ""),
                    }
                )
            return (alias, branches, True)
        except KeboolaApiError as exc:
            return (
                alias,
                {
                    "project_alias": alias,
                    "error_code": exc.error_code,
                    "message": exc.message,
                },
            )
        except Exception as exc:
            return (
                alias,
                {
                    "project_alias": alias,
                    "error_code": "UNEXPECTED_ERROR",
                    "message": str(exc),
                },
            )
        finally:
            client.close()

    def list_branches(
        self,
        aliases: list[str] | None = None,
    ) -> dict[str, Any]:
        """List development branches across one or multiple projects.

        Queries each resolved project for development branches in parallel,
        flattens them into a unified list. Per-project errors are collected
        but do not stop other projects from being queried.

        Args:
            aliases: Project aliases to query. None means all projects.

        Returns:
            Dict with keys:
                - "branches": list of branch dicts with project_alias,
                  id, name, isDefault, created, description
                - "errors": list of error dicts with project_alias,
                  error_code, message

        Raises:
            ConfigError: If a specified alias is not found (before querying).
        """
        projects = self.resolve_projects(aliases)

        def worker(alias: str, project: ProjectConfig) -> tuple[Any, ...]:
            return self._fetch_project_branches(alias, project)

        successes, errors = self._run_parallel(projects, worker)

        # Flatten branches from all successful projects
        all_branches: list[dict[str, Any]] = []
        for _alias, branches, _ok in successes:
            all_branches.extend(branches)

        # Sort for deterministic output
        all_branches.sort(
            key=lambda b: (b["project_alias"], b.get("id", 0))
        )
        errors.sort(key=lambda e: e.get("project_alias", ""))

        return {"branches": all_branches, "errors": errors}
