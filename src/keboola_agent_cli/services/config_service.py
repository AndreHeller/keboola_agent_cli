"""Configuration listing service - business logic for listing and detailing configs.

Orchestrates multi-project configuration retrieval in parallel, filtering,
and aggregation without knowing about CLI or HTTP details.
"""

from typing import Any

from ..errors import KeboolaApiError
from ..models import ProjectConfig
from .base import BaseService


class ConfigService(BaseService):
    """Business logic for listing and inspecting Keboola configurations.

    Supports multi-project aggregation: queries multiple projects in parallel
    using ThreadPoolExecutor, collects results, and reports per-project errors
    without stopping others.

    Uses dependency injection for config_store and client_factory.
    """

    def _fetch_project_configs(
        self,
        alias: str,
        project: ProjectConfig,
        component_type: str | None = None,
        component_id: str | None = None,
    ) -> tuple[str, list[dict[str, Any]], bool] | tuple[str, dict[str, str]]:
        """Fetch configurations for a single project (runs in a worker thread).

        Creates its own KeboolaClient, fetches components and configs, then
        closes the client. Returns either (alias, configs_list, True) on success
        or (alias, error_dict) on failure. The 3-tuple convention is required
        by _run_parallel() which uses tuple length to distinguish success/error.
        """
        client = self._client_factory(project.stack_url, project.token)
        try:
            components = client.list_components(component_type=component_type)
            configs: list[dict[str, Any]] = []
            for component in components:
                comp_id = component.get("id", "")
                comp_name = component.get("name", "")
                comp_type = component.get("type", "")

                # Apply component_id filter if specified
                if component_id and comp_id != component_id:
                    continue

                configurations = component.get("configurations", [])
                for cfg in configurations:
                    configs.append(
                        {
                            "project_alias": alias,
                            "component_id": comp_id,
                            "component_name": comp_name,
                            "component_type": comp_type,
                            "config_id": str(cfg.get("id", "")),
                            "config_name": cfg.get("name", ""),
                            "config_description": cfg.get("description", ""),
                        }
                    )
            return (alias, configs, True)
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

    def list_configs(
        self,
        aliases: list[str] | None = None,
        component_type: str | None = None,
        component_id: str | None = None,
    ) -> dict[str, Any]:
        """List configurations across one or multiple projects.

        Queries each resolved project for components and their configurations
        in parallel, flattens them into a unified list. Per-project errors are
        collected but do not stop other projects from being queried.

        Args:
            aliases: Project aliases to query. None means all projects.
            component_type: Optional filter by component type
                (extractor, writer, transformation, application).
            component_id: Optional filter by specific component ID
                (e.g. keboola.ex-db-snowflake).

        Returns:
            Dict with keys:
                - "configs": list of config dicts with project_alias,
                  component_id, component_name, component_type,
                  config_id, config_name, config_description
                - "errors": list of error dicts with project_alias,
                  error_code, message

        Raises:
            ConfigError: If a specified alias is not found (before querying).
        """
        projects = self.resolve_projects(aliases)

        def worker(alias: str, project: ProjectConfig) -> tuple[Any, ...]:
            return self._fetch_project_configs(alias, project, component_type, component_id)

        successes, errors = self._run_parallel(projects, worker)

        # Flatten configs from all successful projects
        all_configs: list[dict[str, Any]] = []
        for _alias, configs, _ok in successes:
            all_configs.extend(configs)

        # Sort for deterministic output
        all_configs.sort(key=lambda c: (c["project_alias"], c["component_id"], c["config_id"]))
        errors.sort(key=lambda e: e.get("project_alias", ""))

        return {"configs": all_configs, "errors": errors}

    def get_config_detail(
        self,
        alias: str,
        component_id: str,
        config_id: str,
    ) -> dict[str, Any]:
        """Get detailed information about a specific configuration.

        Args:
            alias: Project alias to query.
            component_id: The component ID (e.g. keboola.ex-db-snowflake).
            config_id: The configuration ID.

        Returns:
            Dict with the full configuration detail from the API,
            plus a "project_alias" key.

        Raises:
            ConfigError: If the alias is not found.
            KeboolaApiError: If the API call fails.
        """
        projects = self.resolve_projects([alias])
        project = projects[alias]

        client = self._client_factory(project.stack_url, project.token)
        try:
            detail = client.get_config_detail(component_id, config_id)
        finally:
            client.close()

        detail["project_alias"] = alias
        return detail
