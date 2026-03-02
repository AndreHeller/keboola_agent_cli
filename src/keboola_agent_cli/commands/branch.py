"""Branch commands - list development branches.

Thin CLI layer: parses arguments, calls BranchService, formats output.
No business logic belongs here.
"""

import typer

from ..errors import ConfigError
from ..output import format_branches_table
from ._helpers import emit_project_warnings, get_formatter, get_service

branch_app = typer.Typer(help="Manage development branches")


@branch_app.command("list")
def branch_list(
    ctx: typer.Context,
    project: list[str] | None = typer.Option(
        None,
        "--project",
        help="Project alias to query (can be repeated for multiple projects)",
    ),
) -> None:
    """List development branches from connected projects."""
    formatter = get_formatter(ctx)
    service = get_service(ctx, "branch_service")

    try:
        result = service.list_branches(aliases=project)
    except ConfigError as exc:
        formatter.error(message=exc.message, error_code="CONFIG_ERROR")
        raise typer.Exit(code=5) from None

    if formatter.json_mode:
        formatter.output(result)
    else:
        format_branches_table(formatter.console, result)
        emit_project_warnings(formatter, result)
