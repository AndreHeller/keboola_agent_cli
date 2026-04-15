"""Job commands - list history, show detail, and run jobs via Queue API.

Thin CLI layer: parses arguments, calls JobService, formats output.
No business logic belongs here.
"""

import typer

from ..config_store import ConfigStore
from ..constants import DEFAULT_JOB_LIMIT, DEFAULT_JOB_RUN_TIMEOUT, MAX_JOB_LIMIT, VALID_STATUSES
from ..errors import ConfigError, KeboolaApiError
from ..output import format_job_detail, format_jobs_table
from ._helpers import (
    check_cli_permission,
    emit_hint,
    emit_project_warnings,
    get_formatter,
    get_service,
    map_error_to_exit_code,
    resolve_branch,
    should_hint,
    validate_branch_requires_project,
)

job_app = typer.Typer(help="Browse job history and run jobs")


@job_app.callback(invoke_without_command=True)
def _job_permission_check(ctx: typer.Context) -> None:
    check_cli_permission(ctx, "job")


@job_app.command("list")
def job_list(
    ctx: typer.Context,
    project: list[str] | None = typer.Option(
        None,
        "--project",
        help="Project alias to query (can be repeated for multiple projects)",
    ),
    component_id: str | None = typer.Option(
        None,
        "--component-id",
        help="Filter by component ID (e.g. keboola.ex-db-snowflake)",
    ),
    config_id: str | None = typer.Option(
        None,
        "--config-id",
        help="Filter by configuration ID (requires --component-id)",
    ),
    status: str | None = typer.Option(
        None,
        "--status",
        help="Filter by job status: processing, terminated, cancelled, success, error",
    ),
    limit: int = typer.Option(
        DEFAULT_JOB_LIMIT,
        "--limit",
        help=f"Maximum number of jobs to return per project (1-{MAX_JOB_LIMIT})",
    ),
) -> None:
    """List jobs from connected projects."""
    if should_hint(ctx):
        emit_hint(
            ctx,
            "job.list",
            project=project,
            component_id=component_id,
            config_id=config_id,
            status=status,
            limit=limit,
        )
        return
    formatter = get_formatter(ctx)
    service = get_service(ctx, "job_service")

    # Validate status
    if status and status not in VALID_STATUSES:
        formatter.error(
            message=f"Invalid status '{status}'. Valid statuses: {', '.join(VALID_STATUSES)}",
            error_code="INVALID_ARGUMENT",
        )
        raise typer.Exit(code=2)

    # Validate limit range
    if limit < 1 or limit > MAX_JOB_LIMIT:
        formatter.error(
            message=f"Invalid limit {limit}. Must be between 1 and {MAX_JOB_LIMIT}.",
            error_code="INVALID_ARGUMENT",
        )
        raise typer.Exit(code=2)

    # Validate config_id requires component_id
    if config_id and not component_id:
        formatter.error(
            message="--config-id requires --component-id to be specified.",
            error_code="INVALID_ARGUMENT",
        )
        raise typer.Exit(code=2)

    try:
        result = service.list_jobs(
            aliases=project,
            component_id=component_id,
            config_id=config_id,
            status=status,
            limit=limit,
        )
    except ConfigError as exc:
        formatter.error(message=exc.message, error_code="CONFIG_ERROR")
        raise typer.Exit(code=5) from None

    if formatter.json_mode:
        formatter.output(result)
    else:
        format_jobs_table(formatter.console, result)
        emit_project_warnings(formatter, result)


@job_app.command("detail")
def job_detail(
    ctx: typer.Context,
    project: str = typer.Option(..., "--project", help="Project alias"),
    job_id: str = typer.Option(..., "--job-id", help="Job ID"),
) -> None:
    """Show detailed information about a specific job."""
    if should_hint(ctx):
        emit_hint(ctx, "job.detail", project=project, job_id=job_id)
        return
    formatter = get_formatter(ctx)
    service = get_service(ctx, "job_service")

    try:
        result = service.get_job_detail(alias=project, job_id=job_id)
        formatter.output(result, format_job_detail)
    except ConfigError as exc:
        formatter.error(message=exc.message, error_code="CONFIG_ERROR")
        raise typer.Exit(code=5) from None
    except KeboolaApiError as exc:
        exit_code = map_error_to_exit_code(exc)
        formatter.error(
            message=exc.message,
            error_code=exc.error_code,
            project=project,
            retryable=exc.retryable,
        )
        raise typer.Exit(code=exit_code) from None


@job_app.command("run")
def job_run(
    ctx: typer.Context,
    project: str = typer.Option(
        ...,
        "--project",
        help="Project alias",
    ),
    component_id: str = typer.Option(
        ...,
        "--component-id",
        help="Component ID (e.g. keboola.snowflake-transformation)",
    ),
    config_id: str = typer.Option(
        ...,
        "--config-id",
        help="Configuration ID",
    ),
    row_id: list[str] | None = typer.Option(
        None,
        "--row-id",
        help="Config row ID(s) to run (repeat for multiple; omit to run entire config)",
    ),
    wait: bool = typer.Option(
        False,
        "--wait",
        help="Wait for job to finish (poll until terminal state)",
    ),
    timeout: float = typer.Option(
        DEFAULT_JOB_RUN_TIMEOUT,
        "--timeout",
        help="Max seconds to wait when --wait is set",
    ),
    branch: int | None = typer.Option(
        None,
        "--branch",
        help="Dev branch ID (overrides active branch)",
    ),
) -> None:
    """Run a job for a component configuration.

    Creates a Queue API job and optionally waits for completion.
    Use --row-id to run specific configuration rows.

    When a dev branch is active (via 'branch use'), the job automatically
    runs on that branch. Use --branch to override.
    """
    if should_hint(ctx):
        emit_hint(
            ctx,
            "job.run",
            project=project,
            component_id=component_id,
            config_id=config_id,
            row_id=row_id,
            wait=wait,
            timeout=timeout,
            branch=branch,
        )
        return
    formatter = get_formatter(ctx)
    service = get_service(ctx, "job_service")
    config_store: ConfigStore = ctx.obj["config_store"]

    validate_branch_requires_project(formatter, branch, project)
    _, effective_branch = resolve_branch(config_store, formatter, project, branch)

    if not formatter.json_mode:
        msg = f"Running [cyan]{component_id}[/cyan] / [cyan]{config_id}[/cyan]"
        if row_id:
            msg += f" (rows: {', '.join(row_id)})"
        if effective_branch is not None:
            msg += f" on branch [cyan]{effective_branch}[/cyan]"
        if wait:
            msg += f" [dim](waiting up to {timeout:.0f}s)[/dim]"
        msg += "..."
        formatter.console.print(msg)

    try:
        result = service.run_job(
            alias=project,
            component_id=component_id,
            config_id=config_id,
            config_row_ids=row_id,
            wait=wait,
            timeout=timeout,
            branch_id=effective_branch,
        )
    except ConfigError as exc:
        formatter.error(message=exc.message, error_code="CONFIG_ERROR")
        raise typer.Exit(code=5) from None
    except KeboolaApiError as exc:
        formatter.error(
            message=exc.message,
            error_code=exc.error_code,
            project=project,
            retryable=exc.retryable,
        )
        raise typer.Exit(code=map_error_to_exit_code(exc)) from None

    if formatter.json_mode:
        formatter.output(result)
    else:
        job_id = result.get("id", "?")
        status = result.get("status", "unknown")
        if status in ("success", "terminated"):
            formatter.console.print(f"[bold green]Job {job_id}:[/bold green] {status}")
        elif status == "error":
            error_msg = ""
            job_result = result.get("result", {})
            if isinstance(job_result, dict):
                error_msg = job_result.get("message", "")
            formatter.console.print(f"[bold red]Job {job_id}:[/bold red] {status}")
            if error_msg:
                formatter.console.print(f"  Error: {error_msg}")
            raise typer.Exit(code=1)
        else:
            formatter.console.print(f"[bold blue]Job {job_id}:[/bold blue] {status}")
            if not wait:
                formatter.console.print(
                    "  Use --wait to poll until completion, "
                    f"or: kbagent job detail --project {project} --job-id {job_id}"
                )
