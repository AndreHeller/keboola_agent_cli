"""Explorer command - generate data files and open the kbc-explorer dashboard.

Thin CLI layer: parses arguments, calls ExplorerService, formats output.
No business logic belongs here.
"""

from pathlib import Path
from typing import Optional

import typer

from ..errors import ConfigError
from ._helpers import emit_project_warnings, get_formatter, get_service

explorer_app = typer.Typer(help="Generate and open the KBC Explorer dashboard")


@explorer_app.callback(invoke_without_command=True)
def explorer(
    ctx: typer.Context,
    project: Optional[list[str]] = typer.Option(
        None,
        "--project",
        help="Project alias(es) to include (repeatable, default: all)",
    ),
    output_dir: Optional[Path] = typer.Option(
        None,
        "--output-dir",
        help="Directory to write catalog/orchestration files (default: kbc-explorer/)",
    ),
    job_limit: int = typer.Option(
        500,
        "--job-limit",
        help="Max jobs per project for statistics (default: 500)",
    ),
    tiers: Optional[Path] = typer.Option(
        None,
        "--tiers",
        help="Path to YAML tier config file for project tier assignments",
    ),
    no_open: bool = typer.Option(
        False,
        "--no-open",
        help="Generate files but don't open the browser",
    ),
) -> None:
    """Generate explorer data from connected projects and open the dashboard."""
    formatter = get_formatter(ctx)
    service = get_service(ctx, "explorer_service")

    aliases = project if project else None

    try:
        result = service.generate(
            aliases=aliases,
            output_dir=output_dir,
            job_limit=job_limit,
            open_browser=not no_open,
            tiers_config=tiers,
        )
    except ConfigError as exc:
        formatter.error(message=exc.message, error_code="CONFIG_ERROR")
        raise typer.Exit(code=5) from None

    if formatter.json_mode:
        formatter.output(result)
    else:
        projects_count = result["projects_count"]
        configs_count = result["configs_count"]
        jobs_sampled = result["jobs_sampled"]
        lineage_edges = result["lineage_edges"]
        orch_count = result["orchestrations_count"]
        out_dir = result["output_dir"]

        formatter.console.print(
            f"[bold green]Explorer generated![/bold green] "
            f"{projects_count} projects, {configs_count} configs, "
            f"{jobs_sampled} jobs, {lineage_edges} lineage edges, "
            f"{orch_count} orchestrations"
        )
        formatter.console.print(f"Output: {out_dir}")

        if not no_open:
            formatter.console.print("Opening dashboard in browser...")

        emit_project_warnings(formatter, result)
