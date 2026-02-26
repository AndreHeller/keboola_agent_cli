"""Lineage commands - analyze cross-project data flow via bucket sharing.

Thin CLI layer: parses arguments, calls LineageService, formats output.
No business logic belongs here.
"""

import typer

from ..errors import ConfigError
from ..output import OutputFormatter, format_lineage_table
from ..services.lineage_service import LineageService

lineage_app = typer.Typer(help="Analyze cross-project data lineage via bucket sharing")


def _get_formatter(ctx: typer.Context) -> OutputFormatter:
    """Retrieve the OutputFormatter from the Typer context."""
    return ctx.obj["formatter"]


def _get_service(ctx: typer.Context) -> LineageService:
    """Retrieve the LineageService from the Typer context."""
    return ctx.obj["lineage_service"]


@lineage_app.command("show")
def lineage_show(
    ctx: typer.Context,
    project: list[str] | None = typer.Option(
        None,
        "--project",
        help="Project alias to query (can be repeated for multiple projects)",
    ),
) -> None:
    """Show cross-project data lineage via bucket sharing."""
    formatter = _get_formatter(ctx)
    service = _get_service(ctx)

    try:
        result = service.get_lineage(aliases=project)
    except ConfigError as exc:
        formatter.error(message=exc.message, error_code="CONFIG_ERROR")
        raise typer.Exit(code=5) from None

    if formatter.json_mode:
        formatter.output(result)
    else:
        format_lineage_table(formatter.console, result)

        for err in result.get("errors", []):
            formatter.warning(f"Project '{err['project_alias']}': {err['message']}")


@lineage_app.callback(invoke_without_command=True)
def lineage_callback(ctx: typer.Context) -> None:
    """Default to 'show' when no subcommand is given."""
    if ctx.invoked_subcommand is None:
        ctx.invoke(lineage_show, ctx=ctx, project=None)
