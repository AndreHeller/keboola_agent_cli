"""Workspace service - business logic for workspace lifecycle management.

Orchestrates workspace CRUD, table loading, SQL query execution via Query Service,
and high-level from-transformation workflow. Provides multi-project list and
single-project operations.
"""

import logging
from typing import Any

from ..errors import ConfigError, KeboolaApiError
from ..models import ProjectConfig
from .base import BaseService

logger = logging.getLogger(__name__)


class WorkspaceService(BaseService):
    """Business logic for managing Keboola workspaces.

    Supports:
    - Workspace CRUD (create, list, detail, delete, password reset)
    - Table loading into workspaces
    - SQL query execution via Query Service
    - High-level from-transformation workflow

    Uses dependency injection for config_store and client_factory.
    """

    def _resolve_branch_id(self, alias: str, project: ProjectConfig) -> int:
        """Resolve the effective branch ID for a project.

        Uses active_branch_id if set, otherwise fetches main branch from API.

        Returns:
            Branch ID (int).
        """
        if project.active_branch_id is not None:
            return project.active_branch_id

        client = self._client_factory(project.stack_url, project.token)
        try:
            branches = client.list_dev_branches()
            for branch in branches:
                if branch.get("isDefault", False):
                    return int(branch["id"])
            raise ConfigError(
                f"No default branch found for project '{alias}'. "
                "Set an active branch with 'kbagent branch use'."
            )
        finally:
            client.close()

    def create_workspace(
        self,
        alias: str,
        backend: str = "snowflake",
        read_only: bool = True,
    ) -> dict[str, Any]:
        """Create a new workspace in a project.

        IMPORTANT: The password is only available in the creation response.
        It cannot be retrieved later (only reset).

        Args:
            alias: Project alias.
            backend: Workspace backend (snowflake, bigquery, etc.).
            read_only: Whether the workspace has read-only storage access.

        Returns:
            Dict with workspace details including connection credentials.
        """
        projects = self.resolve_projects([alias])
        project = projects[alias]

        client = self._client_factory(project.stack_url, project.token)
        try:
            ws_data = client.create_workspace(backend=backend, read_only=read_only)
        finally:
            client.close()

        connection = ws_data.get("connection", {})
        return {
            "project_alias": alias,
            "workspace_id": ws_data.get("id"),
            "backend": connection.get("backend", backend),
            "host": connection.get("host", ""),
            "warehouse": connection.get("warehouse", ""),
            "database": connection.get("database", ""),
            "schema": connection.get("schema", ""),
            "user": connection.get("user", ""),
            "password": connection.get("password", ""),
            "read_only": read_only,
            "message": (
                f"Workspace {ws_data.get('id')} created in project '{alias}'. "
                "Save the password -- it cannot be retrieved later!"
            ),
        }

    def list_workspaces(
        self,
        aliases: list[str] | None = None,
    ) -> dict[str, Any]:
        """List workspaces across one or multiple projects.

        Args:
            aliases: Project aliases to query. None means all projects.

        Returns:
            Dict with "workspaces" and "errors" lists.
        """
        projects = self.resolve_projects(aliases)

        def worker(
            alias: str, project: ProjectConfig
        ) -> tuple[str, list[dict[str, Any]], bool] | tuple[str, dict[str, str]]:
            client = self._client_factory(project.stack_url, project.token)
            try:
                raw_workspaces = client.list_workspaces()
                workspaces: list[dict[str, Any]] = []
                for ws in raw_workspaces:
                    connection = ws.get("connection", {})
                    workspaces.append(
                        {
                            "project_alias": alias,
                            "id": ws.get("id"),
                            "backend": connection.get("backend", ""),
                            "host": connection.get("host", ""),
                            "schema": connection.get("schema", ""),
                            "user": connection.get("user", ""),
                            "created": ws.get("created", ""),
                            "component_id": ws.get("configurationId", {}).get("component", "")
                            if isinstance(ws.get("configurationId"), dict)
                            else "",
                            "config_id": ws.get("configurationId", {}).get("config", "")
                            if isinstance(ws.get("configurationId"), dict)
                            else "",
                        }
                    )
                return (alias, workspaces, True)
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

        successes, errors = self._run_parallel(projects, worker)

        all_workspaces: list[dict[str, Any]] = []
        for _alias, workspaces, _ok in successes:
            all_workspaces.extend(workspaces)

        all_workspaces.sort(key=lambda w: (w["project_alias"], w.get("id", 0)))
        errors.sort(key=lambda e: e.get("project_alias", ""))

        return {
            "workspaces": all_workspaces,
            "errors": errors,
        }

    def get_workspace(self, alias: str, workspace_id: int) -> dict[str, Any]:
        """Get workspace details (password NOT included).

        Args:
            alias: Project alias.
            workspace_id: Workspace ID.

        Returns:
            Dict with workspace details.
        """
        projects = self.resolve_projects([alias])
        project = projects[alias]

        client = self._client_factory(project.stack_url, project.token)
        try:
            ws_data = client.get_workspace(workspace_id)
        finally:
            client.close()

        connection = ws_data.get("connection", {})
        return {
            "project_alias": alias,
            "workspace_id": ws_data.get("id"),
            "backend": connection.get("backend", ""),
            "host": connection.get("host", ""),
            "warehouse": connection.get("warehouse", ""),
            "database": connection.get("database", ""),
            "schema": connection.get("schema", ""),
            "user": connection.get("user", ""),
            "created": ws_data.get("created", ""),
        }

    def delete_workspace(self, alias: str, workspace_id: int) -> dict[str, Any]:
        """Delete a workspace.

        Args:
            alias: Project alias.
            workspace_id: Workspace ID.

        Returns:
            Dict confirming the deletion.
        """
        projects = self.resolve_projects([alias])
        project = projects[alias]

        client = self._client_factory(project.stack_url, project.token)
        try:
            client.delete_workspace(workspace_id)
        finally:
            client.close()

        return {
            "project_alias": alias,
            "workspace_id": workspace_id,
            "message": f"Workspace {workspace_id} deleted from project '{alias}'.",
        }

    def reset_password(self, alias: str, workspace_id: int) -> dict[str, Any]:
        """Reset workspace password.

        Args:
            alias: Project alias.
            workspace_id: Workspace ID.

        Returns:
            Dict with the new password.
        """
        projects = self.resolve_projects([alias])
        project = projects[alias]

        client = self._client_factory(project.stack_url, project.token)
        try:
            result = client.reset_workspace_password(workspace_id)
        finally:
            client.close()

        return {
            "project_alias": alias,
            "workspace_id": workspace_id,
            "password": result.get("password", ""),
            "message": (
                f"Password reset for workspace {workspace_id} in project '{alias}'. "
                "Save the new password -- it cannot be retrieved later!"
            ),
        }

    def load_tables(
        self,
        alias: str,
        workspace_id: int,
        tables: list[str],
    ) -> dict[str, Any]:
        """Load tables into a workspace.

        Builds table mapping from table IDs, using the last segment as the
        destination name. Waits for the async storage job to complete.

        Args:
            alias: Project alias.
            workspace_id: Workspace ID.
            tables: List of table IDs (e.g. "in.c-bucket.table-name").

        Returns:
            Dict with load job results.
        """
        projects = self.resolve_projects([alias])
        project = projects[alias]

        # Build table load definitions
        table_defs: list[dict[str, Any]] = []
        for table_id in tables:
            # Use last part of table ID as destination name
            parts = table_id.split(".")
            destination = parts[-1] if parts else table_id
            table_defs.append(
                {
                    "source": table_id,
                    "destination": destination,
                }
            )

        client = self._client_factory(project.stack_url, project.token)
        try:
            job_result = client.load_workspace_tables(workspace_id, table_defs)
        finally:
            client.close()

        return {
            "project_alias": alias,
            "workspace_id": workspace_id,
            "tables_loaded": len(tables),
            "table_ids": tables,
            "job_id": job_result.get("id"),
            "job_status": job_result.get("status", ""),
            "message": f"Loaded {len(tables)} table(s) into workspace {workspace_id}.",
        }

    def execute_query(
        self,
        alias: str,
        workspace_id: int,
        sql: str,
        transactional: bool = False,
    ) -> dict[str, Any]:
        """Execute SQL query in a workspace via Query Service.

        Submits the query, polls until complete, and exports CSV results
        for each statement.

        Args:
            alias: Project alias.
            workspace_id: Workspace ID.
            sql: SQL statement(s) to execute.
            transactional: Whether to wrap in a transaction.

        Returns:
            Dict with query results.
        """
        projects = self.resolve_projects([alias])
        project = projects[alias]
        branch_id = self._resolve_branch_id(alias, project)

        client = self._client_factory(project.stack_url, project.token)
        try:
            # Submit query
            query_job = client.submit_query(
                branch_id=branch_id,
                workspace_id=workspace_id,
                statements=[sql],
                transactional=transactional,
            )

            query_job_id = str(query_job.get("id", ""))

            # Wait for completion
            completed_job = client.wait_for_query_job(query_job_id)

            # Export results for each statement
            results: list[dict[str, Any]] = []
            statements = completed_job.get("statements", [])
            for stmt in statements:
                stmt_id = str(stmt.get("id", ""))
                status = stmt.get("status", "")
                result_entry: dict[str, Any] = {
                    "statement_id": stmt_id,
                    "status": status,
                    "rows_affected": stmt.get("resultRows", 0),
                }

                # Try to export results if there are rows
                if status == "completed" and stmt.get("resultRows", 0) > 0:
                    try:
                        csv_data = client.export_query_results(query_job_id, stmt_id)
                        result_entry["csv_data"] = csv_data
                    except KeboolaApiError:
                        logger.debug("Could not export results for statement %s", stmt_id)

                results.append(result_entry)

            return {
                "project_alias": alias,
                "workspace_id": workspace_id,
                "branch_id": branch_id,
                "query_job_id": query_job_id,
                "status": completed_job.get("status", ""),
                "statements": results,
                "message": f"Query executed in workspace {workspace_id}.",
            }
        finally:
            client.close()

    def create_from_transformation(
        self,
        alias: str,
        component_id: str,
        config_id: str,
        row_id: str | None = None,
        backend: str = "snowflake",
    ) -> dict[str, Any]:
        """Create a workspace from a transformation config.

        Reads the transformation configuration, extracts input table mappings,
        creates a config-tied workspace, and loads the input tables.

        Args:
            alias: Project alias.
            component_id: Transformation component ID (e.g. keboola.snowflake-transformation).
            config_id: Configuration ID.
            row_id: Optional row ID for row-based transformations.
            backend: Workspace backend.

        Returns:
            Dict with workspace details and loaded tables.
        """
        projects = self.resolve_projects([alias])
        project = projects[alias]
        branch_id = self._resolve_branch_id(alias, project)

        client = self._client_factory(project.stack_url, project.token)
        try:
            # Read the transformation config
            config_data = client.get_config_detail(component_id, config_id)

            # Extract input mapping from configuration
            configuration = config_data.get("configuration", {})

            # If row_id specified, find the row
            if row_id:
                rows = config_data.get("rows", [])
                target_row = None
                for row in rows:
                    if str(row.get("id", "")) == str(row_id):
                        target_row = row
                        break
                if target_row is None:
                    raise ConfigError(
                        f"Row '{row_id}' not found in config '{config_id}' "
                        f"of component '{component_id}'."
                    )
                configuration = target_row.get("configuration", {})

            storage = configuration.get("storage", {})
            input_tables = storage.get("input", {}).get("tables", [])

            if not input_tables:
                raise ConfigError(
                    f"No input tables found in transformation config '{config_id}'. "
                    "The configuration may not have input mapping defined."
                )

            # Create config-tied workspace
            ws_data = client.create_config_workspace(
                branch_id=branch_id,
                component_id=component_id,
                config_id=config_id,
                backend=backend,
            )

            workspace_id = ws_data.get("id")
            connection = ws_data.get("connection", {})

            # Build table load definitions from input mapping
            table_defs: list[dict[str, Any]] = []
            source_tables: list[str] = []
            for table in input_tables:
                source = table.get("source", "")
                destination = table.get("destination", "")
                if source:
                    source_tables.append(source)
                    entry: dict[str, Any] = {"source": source, "destination": destination}
                    # Pass through columns, where_column, where_values if present
                    if table.get("columns"):
                        entry["columns"] = table["columns"]
                    if table.get("where_column"):
                        entry["where_column"] = table["where_column"]
                    if table.get("where_values"):
                        entry["where_values"] = table["where_values"]
                    table_defs.append(entry)

            # Load tables into workspace
            if table_defs:
                client.load_workspace_tables(workspace_id, table_defs)

            return {
                "project_alias": alias,
                "workspace_id": workspace_id,
                "branch_id": branch_id,
                "component_id": component_id,
                "config_id": config_id,
                "row_id": row_id,
                "backend": connection.get("backend", backend),
                "host": connection.get("host", ""),
                "warehouse": connection.get("warehouse", ""),
                "database": connection.get("database", ""),
                "schema": connection.get("schema", ""),
                "user": connection.get("user", ""),
                "password": connection.get("password", ""),
                "tables_loaded": source_tables,
                "message": (
                    f"Workspace {workspace_id} created from transformation "
                    f"'{config_id}' with {len(source_tables)} table(s) loaded. "
                    "Save the password -- it cannot be retrieved later!"
                ),
            }
        finally:
            client.close()
