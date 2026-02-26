"""MCP integration service - wraps keboola-mcp-server as subprocess.

Provides multi-project tool listing and execution via MCP protocol.
Read tools run across ALL projects in parallel; write tools target a single project.
"""

import asyncio
import json
import shutil
import subprocess
from contextlib import AsyncExitStack
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from ..config_store import ConfigStore
from ..errors import ConfigError
from ..models import ProjectConfig

# Timeout for individual MCP operations (seconds)
MCP_TOOL_TIMEOUT_SECONDS = 60
# Timeout for MCP session initialization (seconds)
MCP_INIT_TIMEOUT_SECONDS = 30

# Prefixes that indicate write/mutating tools
WRITE_PREFIXES = (
    "create_",
    "update_",
    "delete_",
    "add_",
    "remove_",
    "set_",
)


def _is_write_tool(tool_name: str) -> bool:
    """Determine if a tool name indicates a write/mutating operation."""
    return tool_name.startswith(WRITE_PREFIXES)


def detect_mcp_server_command() -> list[str] | None:
    """Detect the best way to run keboola-mcp-server.

    Checks in order:
    1. uvx keboola_mcp_server (if uvx is available)
    2. keboola_mcp_server (if installed as standalone command)
    3. python -m keboola_mcp_server (last resort)

    Returns:
        List of command parts, or None if no method is available.
    """
    if shutil.which("uvx"):
        return ["uvx", "keboola_mcp_server"]
    if shutil.which("keboola_mcp_server"):
        return ["keboola_mcp_server"]
    if shutil.which("python"):
        return ["python", "-m", "keboola_mcp_server"]
    return None


def _build_server_params(project: ProjectConfig) -> StdioServerParameters:
    """Build StdioServerParameters for a project's MCP server.

    Args:
        project: Project config with stack_url and token.

    Returns:
        StdioServerParameters configured for the project.

    Raises:
        ConfigError: If no MCP server command is available.
    """
    command_parts = detect_mcp_server_command()
    if command_parts is None:
        raise ConfigError(
            "Cannot find keboola-mcp-server. "
            "Install it with: pip install keboola-mcp-server (or: uvx keboola_mcp_server)"
        )

    return StdioServerParameters(
        command=command_parts[0],
        args=[*command_parts[1:], "--transport", "stdio"],
        env={
            "KBC_STORAGE_TOKEN": project.token,
            "KBC_STORAGE_API_URL": project.stack_url,
        },
    )


async def _connect_and_list_tools(
    project: ProjectConfig,
) -> list[dict[str, Any]]:
    """Connect to MCP server for a project and list available tools.

    Args:
        project: Project config.

    Returns:
        List of tool dicts with name, description, inputSchema.
    """
    params = _build_server_params(project)
    exit_stack = AsyncExitStack()

    try:
        read_stream, write_stream = await asyncio.wait_for(
            exit_stack.enter_async_context(
                stdio_client(params, errlog=subprocess.DEVNULL)
            ),
            timeout=MCP_INIT_TIMEOUT_SECONDS,
        )

        session = await exit_stack.enter_async_context(
            ClientSession(read_stream, write_stream)
        )
        await asyncio.wait_for(session.initialize(), timeout=MCP_INIT_TIMEOUT_SECONDS)

        response = await asyncio.wait_for(
            session.list_tools(), timeout=MCP_TOOL_TIMEOUT_SECONDS
        )

        tools = []
        for tool in response.tools:
            tools.append(
                {
                    "name": tool.name,
                    "description": tool.description or "",
                    "inputSchema": tool.inputSchema if tool.inputSchema else {},
                }
            )
        return tools
    finally:
        await exit_stack.aclose()


async def _connect_and_call_tool(
    project: ProjectConfig,
    tool_name: str,
    tool_input: dict[str, Any],
) -> dict[str, Any]:
    """Connect to MCP server for a project and call a specific tool.

    Args:
        project: Project config.
        tool_name: Name of the tool to call.
        tool_input: Input arguments for the tool.

    Returns:
        Dict with tool result content and error status.
    """
    params = _build_server_params(project)
    exit_stack = AsyncExitStack()

    try:
        read_stream, write_stream = await asyncio.wait_for(
            exit_stack.enter_async_context(
                stdio_client(params, errlog=subprocess.DEVNULL)
            ),
            timeout=MCP_INIT_TIMEOUT_SECONDS,
        )

        session = await exit_stack.enter_async_context(
            ClientSession(read_stream, write_stream)
        )
        await asyncio.wait_for(session.initialize(), timeout=MCP_INIT_TIMEOUT_SECONDS)

        result = await asyncio.wait_for(
            session.call_tool(tool_name, tool_input),
            timeout=MCP_TOOL_TIMEOUT_SECONDS,
        )

        # Extract content from result
        content_items = []
        for item in result.content:
            if hasattr(item, "text"):
                # Try to parse as JSON for structured content
                try:
                    content_items.append(json.loads(item.text))
                except (json.JSONDecodeError, TypeError):
                    content_items.append(item.text)
            else:
                content_items.append(str(item))

        return {
            "content": content_items,
            "isError": bool(result.isError),
        }
    finally:
        await exit_stack.aclose()


class McpService:
    """Business logic for MCP tool operations across projects.

    Wraps keboola-mcp-server as subprocess via MCP SDK.
    Read tools execute across all projects in parallel.
    Write tools target a single project.

    Uses the same DI pattern as JobService/ConfigService.
    """

    def __init__(self, config_store: ConfigStore) -> None:
        self._config_store = config_store

    def resolve_projects(self, aliases: list[str] | None = None) -> dict[str, ProjectConfig]:
        """Resolve project aliases to ProjectConfig instances.

        Args:
            aliases: Specific project aliases. If None, returns all.

        Returns:
            Dict mapping alias to ProjectConfig.

        Raises:
            ConfigError: If any specified alias is not found.
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

    def resolve_project(self, alias: str | None = None) -> tuple[str, ProjectConfig]:
        """Resolve a single project alias (or the default project).

        Args:
            alias: Project alias. If None, uses default_project.

        Returns:
            Tuple of (alias, ProjectConfig).

        Raises:
            ConfigError: If alias not found or no default project set.
        """
        config = self._config_store.load()

        if alias is None:
            alias = config.default_project
            if not alias:
                raise ConfigError(
                    "No default project set. Use 'kbagent project add' or specify --project."
                )

        if alias not in config.projects:
            raise ConfigError(f"Project '{alias}' not found.")

        return alias, config.projects[alias]

    def list_tools(
        self, aliases: list[str] | None = None
    ) -> dict[str, Any]:
        """List available MCP tools from the first reachable project.

        Tools are the same across projects (same MCP server), so we only
        need to query one project. Each tool is annotated with multi_project
        flag based on read/write classification.

        Args:
            aliases: Project aliases. Uses first available if None.

        Returns:
            Dict with "tools" list and "errors" list.

        Raises:
            ConfigError: If no projects are configured.
        """
        projects = self.resolve_projects(aliases)

        if not projects:
            raise ConfigError("No projects configured. Use 'kbagent project add' first.")

        # Try each project until one succeeds
        errors: list[dict[str, str]] = []
        for alias, project in projects.items():
            try:
                tools = asyncio.run(_connect_and_list_tools(project))
                # Annotate tools with multi_project flag
                annotated_tools = []
                for tool in tools:
                    tool["multi_project"] = not _is_write_tool(tool["name"])
                    annotated_tools.append(tool)

                return {"tools": annotated_tools, "errors": errors}
            except Exception as exc:
                errors.append(
                    {
                        "project_alias": alias,
                        "error_code": "MCP_ERROR",
                        "message": f"Failed to list tools: {exc}",
                    }
                )

        return {"tools": [], "errors": errors}

    def get_tool_schema(
        self, tool_name: str, aliases: list[str] | None = None
    ) -> dict[str, Any] | None:
        """Get the input schema for a specific tool.

        Fetches tool list from the first available project and finds the
        matching tool's inputSchema.

        Args:
            tool_name: Name of the tool.
            aliases: Project aliases to try.

        Returns:
            The tool's inputSchema dict, or None if tool not found.
        """
        result = self.list_tools(aliases=aliases)
        for tool in result.get("tools", []):
            if tool["name"] == tool_name:
                return tool.get("inputSchema", {})
        return None

    def validate_tool_input(
        self,
        tool_name: str,
        tool_input: dict[str, Any],
        aliases: list[str] | None = None,
    ) -> list[str]:
        """Validate tool input against the tool's schema.

        Checks that all required parameters are provided.

        Args:
            tool_name: Name of the tool.
            tool_input: Input arguments to validate.
            aliases: Project aliases to try for schema lookup.

        Returns:
            List of missing required parameter names. Empty if all OK.
        """
        schema = self.get_tool_schema(tool_name, aliases=aliases)
        if schema is None:
            return []  # Can't validate - tool not found, let it fail at call time

        required = schema.get("required", [])
        missing = [param for param in required if param not in tool_input]
        return missing

    def call_tool(
        self,
        tool_name: str,
        tool_input: dict[str, Any] | None = None,
        alias: str | None = None,
    ) -> dict[str, Any]:
        """Call an MCP tool.

        For read tools (multi_project=true): runs across ALL projects in parallel,
        aggregates results with project_alias annotation.

        For write tools: runs on a single project (specified by alias or default).

        Args:
            tool_name: Name of the MCP tool to call.
            tool_input: Input arguments for the tool.
            alias: Project alias for write tools. Ignored for read tools
                   unless specified to limit scope.

        Returns:
            Dict with "results" list and "errors" list.
        """
        if tool_input is None:
            tool_input = {}

        is_write = _is_write_tool(tool_name)

        if is_write:
            return self._call_write_tool(tool_name, tool_input, alias)
        else:
            return self._call_read_tool(tool_name, tool_input, alias)

    def _call_write_tool(
        self,
        tool_name: str,
        tool_input: dict[str, Any],
        alias: str | None,
    ) -> dict[str, Any]:
        """Execute a write tool on a single project."""
        resolved_alias, project = self.resolve_project(alias)

        try:
            result = asyncio.run(_connect_and_call_tool(project, tool_name, tool_input))
            result["project_alias"] = resolved_alias
            return {"results": [result], "errors": []}
        except Exception as exc:
            return {
                "results": [],
                "errors": [
                    {
                        "project_alias": resolved_alias,
                        "error_code": "MCP_ERROR",
                        "message": str(exc),
                    }
                ],
            }

    def _call_read_tool(
        self,
        tool_name: str,
        tool_input: dict[str, Any],
        alias: str | None,
    ) -> dict[str, Any]:
        """Execute a read tool across projects in parallel."""
        projects = self.resolve_projects([alias]) if alias else self.resolve_projects()

        if not projects:
            raise ConfigError("No projects configured. Use 'kbagent project add' first.")

        return asyncio.run(self._gather_read_results(projects, tool_name, tool_input))

    async def _gather_read_results(
        self,
        projects: dict[str, ProjectConfig],
        tool_name: str,
        tool_input: dict[str, Any],
    ) -> dict[str, Any]:
        """Run a read tool across multiple projects in parallel using asyncio.gather."""
        tasks = {}
        for a, project in projects.items():
            tasks[a] = asyncio.create_task(
                _connect_and_call_tool(project, tool_name, tool_input)
            )

        all_results: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []

        for a, task in tasks.items():
            try:
                result = await task
                result["project_alias"] = a
                all_results.append(result)
            except Exception as exc:
                errors.append(
                    {
                        "project_alias": a,
                        "error_code": "MCP_ERROR",
                        "message": str(exc),
                    }
                )

        return {"results": all_results, "errors": errors}

    def check_server_available(self) -> dict[str, Any]:
        """Check if MCP server is available (for doctor command).

        Returns:
            Dict with check status and message.
        """
        command = detect_mcp_server_command()
        if command is None:
            return {
                "check": "mcp_server",
                "name": "MCP server",
                "status": "warn",
                "message": (
                    "keboola-mcp-server not found. "
                    "Install with: pip install keboola-mcp-server "
                    "(or use uvx keboola_mcp_server). "
                    "MCP tool commands will not work without it."
                ),
            }

        return {
            "check": "mcp_server",
            "name": "MCP server",
            "status": "pass",
            "message": f"MCP server available via: {' '.join(command)}",
        }
