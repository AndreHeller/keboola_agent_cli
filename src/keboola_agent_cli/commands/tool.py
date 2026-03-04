"""MCP tool commands - list and call tools from keboola-mcp-server.

Thin CLI layer: parses arguments, calls McpService, formats output.
No business logic belongs here.
"""

import json

import typer

from ..config_store import ConfigStore
from ..errors import ConfigError
from ..output import OutputFormatter, format_tool_result, format_tools_table
from ._helpers import emit_project_warnings, get_formatter, get_service

tool_app = typer.Typer(help="MCP tools - interact with Keboola via MCP server")


def _validate_branch_requires_project(
    formatter: OutputFormatter,
    branch: int | None,
    project: str | None,
) -> None:
    """Validate that --branch is always accompanied by --project.

    Raises:
        typer.Exit: With code 2 if branch is set but project is not.
    """
    if branch is not None and not project:
        formatter.error(
            message="--branch requires --project (branch ID is per-project)",
            error_code="INVALID_ARGUMENT",
        )
        raise typer.Exit(code=2) from None


def _resolve_branch(
    config_store: ConfigStore,
    formatter: OutputFormatter,
    project: str | None,
    branch: int | None,
) -> tuple[str | None, str | None]:
    """Resolve the effective branch and project for tool commands.

    Resolution order:
    1. Explicit --branch always wins (no change)
    2. If no --branch, check active_branch_id from config for the resolved project
    3. If active branch found, use it and print info message in human mode

    When an active branch is resolved from config, --project is also set
    to the project alias (branch is per-project).

    Args:
        config_store: Config store for looking up project configs.
        formatter: Output formatter for info messages.
        project: Explicit --project alias or None.
        branch: Explicit --branch integer or None.

    Returns:
        Tuple of (effective_project, effective_branch_as_str).
    """
    if branch is not None:
        return project, str(branch)

    if project is not None:
        # Specific project requested - check its active branch
        proj_config = config_store.get_project(project)
        if proj_config and proj_config.active_branch_id is not None:
            branch_id_str = str(proj_config.active_branch_id)
            if not formatter.json_mode:
                formatter.err_console.print(
                    f"[bold blue]Info:[/bold blue] Using active branch "
                    f"(ID: {proj_config.active_branch_id}) for project '{project}'"
                )
            return project, branch_id_str
    else:
        # No project specified - check all projects for an active branch.
        # If exactly one project has an active branch, use it to avoid ambiguity.
        config = config_store.load()
        active_projects = [
            (alias, proj)
            for alias, proj in config.projects.items()
            if proj.active_branch_id is not None
        ]
        if len(active_projects) == 1:
            alias, proj = active_projects[0]
            branch_id_str = str(proj.active_branch_id)
            if not formatter.json_mode:
                formatter.err_console.print(
                    f"[bold blue]Info:[/bold blue] Using active branch "
                    f"(ID: {proj.active_branch_id}) for project '{alias}'"
                )
            return alias, branch_id_str

    return project, None


@tool_app.command("list")
def tool_list(
    ctx: typer.Context,
    project: str | None = typer.Option(
        None,
        "--project",
        help="Project alias to query tools from (uses first available if not set)",
    ),
    branch: int | None = typer.Option(
        None,
        "--branch",
        help="Development branch ID (requires --project or active branch)",
    ),
) -> None:
    """List available MCP tools from the keboola-mcp-server."""
    formatter = get_formatter(ctx)
    service = get_service(ctx, "mcp_service")
    config_store: ConfigStore = ctx.obj["config_store"]

    _validate_branch_requires_project(formatter, branch, project)

    # Auto-resolve active branch from config
    project, branch_str = _resolve_branch(config_store, formatter, project, branch)

    if branch_str and not project:
        formatter.error(
            message="--branch requires --project (branch ID is per-project)",
            error_code="INVALID_ARGUMENT",
        )
        raise typer.Exit(code=2) from None

    aliases = [project] if project else None

    try:
        result = service.list_tools(aliases=aliases, branch_id=branch_str)
    except ConfigError as exc:
        formatter.error(message=exc.message, error_code="CONFIG_ERROR")
        raise typer.Exit(code=5) from None

    if formatter.json_mode:
        formatter.output(result)
    else:
        format_tools_table(formatter.console, result)
        emit_project_warnings(formatter, result)


@tool_app.command("call")
def tool_call(
    ctx: typer.Context,
    tool_name: str = typer.Argument(help="Name of the MCP tool to call"),
    project: str | None = typer.Option(
        None,
        "--project",
        help="Project alias (required for write tools, optional for read tools)",
    ),
    tool_input: str | None = typer.Option(
        None,
        "--input",
        help="Tool input as JSON string (e.g. '{\"query\": \"test\"}')",
    ),
    branch: int | None = typer.Option(
        None,
        "--branch",
        help="Development branch ID (forces single-project mode)",
    ),
) -> None:
    """Call an MCP tool on keboola-mcp-server.

    Read tools (list_*, get_*, search, docs_query, find_*) run across ALL
    connected projects in parallel and return aggregated results.

    Write tools (create_*, update_*, delete_*, add_*) run on a single project.
    Use --project to specify the target, or the default project is used.

    If an active branch is set for the project, it is used automatically.
    Use --branch to override or scope the call to a specific development branch.
    """
    formatter = get_formatter(ctx)
    service = get_service(ctx, "mcp_service")
    config_store: ConfigStore = ctx.obj["config_store"]

    _validate_branch_requires_project(formatter, branch, project)

    # Auto-resolve active branch from config
    project, branch_str = _resolve_branch(config_store, formatter, project, branch)

    if branch_str and not project:
        formatter.error(
            message="--branch requires --project (branch ID is per-project)",
            error_code="INVALID_ARGUMENT",
        )
        raise typer.Exit(code=2) from None

    # Parse tool input JSON
    parsed_input: dict = {}
    if tool_input:
        try:
            parsed_input = json.loads(tool_input)
        except json.JSONDecodeError as exc:
            formatter.error(
                message=f"Invalid JSON in --input: {exc}",
                error_code="INVALID_ARGUMENT",
            )
            raise typer.Exit(code=2) from None

        if not isinstance(parsed_input, dict):
            formatter.error(
                message="--input must be a JSON object (e.g. '{\"key\": \"value\"}')",
                error_code="INVALID_ARGUMENT",
            )
            raise typer.Exit(code=2) from None

    # Validate required parameters before calling tool across all projects
    try:
        missing, known_tools = service.validate_tool_input(
            tool_name=tool_name,
            tool_input=parsed_input,
            aliases=[project] if project else None,
            branch_id=branch_str,
        )
    except ConfigError as exc:
        formatter.error(message=exc.message, error_code="CONFIG_ERROR")
        raise typer.Exit(code=5) from None

    if missing:
        params_str = ", ".join(missing)
        example_json = json.dumps({p: "..." for p in missing})
        formatter.error(
            message=(
                f"Missing required parameter(s) for '{tool_name}': {params_str}. "
                f"Use: kbagent tool call {tool_name} --input '{example_json}'"
            ),
            error_code="MISSING_PARAMETER",
        )
        raise typer.Exit(code=2) from None

    try:
        result = service.call_tool(
            tool_name=tool_name,
            tool_input=parsed_input,
            alias=project,
            branch_id=branch_str,
            _known_tools=known_tools,
        )
    except ConfigError as exc:
        formatter.error(message=exc.message, error_code="CONFIG_ERROR")
        raise typer.Exit(code=5) from None

    if formatter.json_mode:
        formatter.output(result)
    else:
        format_tool_result(formatter.console, result)
        emit_project_warnings(formatter, result)
