"""Sync commands - init, pull, push, diff, and status for local filesystem sync.

Thin CLI layer: parses arguments, calls SyncService, formats output.
No business logic belongs here.
"""

from pathlib import Path
from typing import Any

import typer

from ..errors import ConfigError, KeboolaApiError
from ._helpers import get_formatter, get_service, map_error_to_exit_code

sync_app = typer.Typer(help="(BETA) Sync project configurations with local filesystem")


def _change_label(change: dict) -> str:
    """Build a human-readable label for a config change entry."""
    path = change.get("path", "")
    name = change.get("config_name", "")
    component = change["component_id"]
    if path:
        return f"{component}/{path}"
    if name:
        return f"{component}/{name}"
    config_id = change.get("config_id", "")
    return f"{component}/{config_id}" if config_id else component


@sync_app.command("init")
def sync_init(
    ctx: typer.Context,
    project: str = typer.Option(
        ...,
        "--project",
        help="Project alias to initialize sync for",
    ),
    directory: Path = typer.Option(
        Path("."),
        "--directory",
        "-d",
        help="Target directory for the project files",
    ),
    git_branching: bool = typer.Option(
        False,
        "--git-branching",
        help="Enable git-branching mode (maps git branches to Keboola branches)",
    ),
) -> None:
    """Initialize a sync working directory for a Keboola project.

    Creates the .keboola/ directory with manifest.json containing
    project metadata and naming conventions. Optionally enables
    git-branching mode for branch-to-branch mapping.
    """
    formatter = get_formatter(ctx)
    service = get_service(ctx, "sync_service")
    project_root = directory.resolve()

    try:
        result = service.init_sync(
            alias=project,
            project_root=project_root,
            git_branching=git_branching,
        )
    except ConfigError as exc:
        formatter.error(message=exc.message, error_code="CONFIG_ERROR")
        raise typer.Exit(code=5) from None
    except FileExistsError as exc:
        formatter.error(message=str(exc), error_code="ALREADY_EXISTS")
        raise typer.Exit(code=1) from None
    except KeboolaApiError as exc:
        formatter.error(
            message=exc.message,
            error_code=exc.error_code,
        )
        raise typer.Exit(code=map_error_to_exit_code(exc)) from None

    if formatter.json_mode:
        formatter.output(result)
    else:
        formatter.success(
            f"Initialized sync for project '{result['project_alias']}' (ID: {result['project_id']})"
        )
        formatter.console.print(f"  API host: {result['api_host']}")
        if result["git_branching"]:
            formatter.console.print(
                f"  Git-branching: enabled (default branch: {result['default_branch']})"
            )
        for f in result["files_created"]:
            formatter.console.print(f"  Created: {f}")


def _format_pull_result(formatter: Any, result: dict) -> None:
    """Format a single-project pull result for human output."""
    is_dry = result.get("status") == "dry_run"
    details = result.get("details", [])
    new_cfgs = [d for d in details if d["action"] == "new"]
    updated_cfgs = [d for d in details if d["action"] == "updated"]
    removed_cfgs = [d for d in details if d["action"] == "removed"]
    skipped_cfgs = [d for d in details if d["action"] == "skipped"]

    has_changes = bool(new_cfgs or updated_cfgs or removed_cfgs)

    if not has_changes and not skipped_cfgs:
        formatter.console.print("[green]Already up to date.[/green] No changes from remote.")
        return
    elif is_dry:
        formatter.console.print("[yellow]Dry run -- no files written:[/yellow]")
        formatter.console.print(
            f"  Would pull {result['configs_pulled']} configurations "
            f"({result['rows_pulled']} rows), "
            f"write {result['files_written']} files"
        )
    else:
        formatter.success(
            f"Pulled {result['configs_pulled']} configurations "
            f"({result['rows_pulled']} rows) "
            f"into {result['branch_dir']}/"
        )
        formatter.console.print(f"  Files written: {result['files_written']}")

    if new_cfgs:
        formatter.console.print(f"  [green]New ({len(new_cfgs)}):[/green]")
        for d in new_cfgs:
            formatter.console.print(f"    + {d['component_id']}/{d['config_name']}")
    if updated_cfgs:
        formatter.console.print(f"  [yellow]Updated ({len(updated_cfgs)}):[/yellow]")
        for d in updated_cfgs:
            formatter.console.print(f"    ~ {d['component_id']}/{d['config_name']}")
    if removed_cfgs:
        formatter.console.print(f"  [red]Removed from remote ({len(removed_cfgs)}):[/red]")
        for d in removed_cfgs:
            formatter.console.print(f"    - {d['path']}")
    if skipped_cfgs:
        formatter.console.print(
            f"  [cyan]Skipped ({len(skipped_cfgs)}) -- locally modified:[/cyan]"
        )
        for d in skipped_cfgs:
            formatter.console.print(f"    ! {d['component_id']}/{d['config_name']}")


def _format_diff_result(formatter: Any, result: dict) -> None:
    """Format a single-project diff result for human output."""
    changes = result["changes"]
    summary = result["summary"]
    remote_only = result.get("remote_only", [])

    if not changes and not remote_only:
        formatter.console.print("[green]No differences.[/green]")
        return

    local_changes = [c for c in changes if c["change_type"] in ("added", "modified", "deleted")]
    remote_changes = [c for c in changes if c["change_type"] == "remote_modified"]
    conflict_changes = [c for c in changes if c["change_type"] == "conflict"]

    if local_changes:
        for change in local_changes:
            ct = change["change_type"]
            label = _change_label(change)
            prefix = {"added": "+", "modified": "~", "deleted": "-"}.get(ct, "?")
            formatter.console.print(f"  {prefix} {ct.upper()} {label}")
        formatter.console.print(
            f"  {summary['added']} to create, {summary['modified']} to update, "
            f"{summary['deleted']} to delete"
        )
    if remote_changes:
        for change in remote_changes:
            formatter.console.print(f"  ~ REMOTE MODIFIED {_change_label(change)}")
    if conflict_changes:
        for change in conflict_changes:
            formatter.console.print(f"  ! CONFLICT {_change_label(change)}")
    if remote_only:
        formatter.console.print(f"  {len(remote_only)} new remote-only config(s)")


def _format_push_result(formatter: Any, result: dict) -> None:
    """Format a single-project push result for human output."""
    status = result.get("status", "")
    if status == "no_changes":
        formatter.console.print("  No changes to push.")
        return
    if status == "dry_run":
        summary = result.get("summary", {})
        formatter.console.print(
            f"  Would create {summary.get('added', 0)}, "
            f"update {summary.get('modified', 0)}, "
            f"delete {summary.get('deleted', 0)}"
        )
        return
    formatter.console.print(
        f"  {result.get('created', 0)} created, "
        f"{result.get('updated', 0)} updated, "
        f"{result.get('deleted', 0)} deleted"
    )


def _format_all_results(
    formatter: Any,
    data: dict,
    per_project_formatter: Any = None,
) -> None:
    """Format multi-project results for human output."""
    summary = data["summary"]
    projects = data["projects"]
    skipped = data.get("skipped", [])

    for alias in sorted(projects):
        proj_result = projects[alias]
        if "error" in proj_result:
            formatter.console.print(
                f"\n[bold]{alias}:[/bold] [red]ERROR: {proj_result['error']}[/red]"
            )
        else:
            formatter.console.print(f"\n[bold]{alias}:[/bold]")
            if per_project_formatter:
                per_project_formatter(formatter, proj_result)

    if skipped:
        formatter.console.print(f"\n[dim]Skipped (no manifest): {', '.join(skipped)}[/dim]")

    formatter.console.print(
        f"\n{summary['total']} projects: "
        f"{summary['success']} OK, {summary['failed']} failed"
        + (f", {summary.get('skipped', 0)} skipped" if summary.get("skipped") else "")
    )


@sync_app.command("pull")
def sync_pull(
    ctx: typer.Context,
    project: str | None = typer.Option(
        None,
        "--project",
        help="Project alias to pull configurations from",
    ),
    all_projects: bool = typer.Option(
        False,
        "--all-projects",
        help="Pull all configured projects in parallel",
    ),
    directory: Path = typer.Option(
        Path("."),
        "--directory",
        "-d",
        help="Project root directory (must contain .keboola/)",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Overwrite local files without checking for modifications",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would be pulled without writing any files",
    ),
) -> None:
    """Download configurations from a Keboola project to local files.

    Use --project for a single project or --all-projects for all configured
    projects in parallel (each in its own subdirectory).
    """
    formatter = get_formatter(ctx)
    service = get_service(ctx, "sync_service")

    if all_projects and project:
        formatter.error(
            message="Cannot use --project with --all-projects",
            error_code="USAGE_ERROR",
        )
        raise typer.Exit(code=2)
    if not all_projects and not project:
        formatter.error(
            message="Specify --project ALIAS or --all-projects",
            error_code="USAGE_ERROR",
        )
        raise typer.Exit(code=2)

    if all_projects:
        base_dir = directory.resolve()
        try:
            data = service.pull_all(base_dir, force=force, dry_run=dry_run)
        except ConfigError as exc:
            formatter.error(message=exc.message, error_code="CONFIG_ERROR")
            raise typer.Exit(code=5) from None

        if formatter.json_mode:
            formatter.output(data)
        else:
            _format_all_results(formatter, data, _format_pull_result)
        return

    project_root = directory.resolve()

    try:
        result = service.pull(
            alias=project,
            project_root=project_root,
            force=force,
            dry_run=dry_run,
        )
    except FileNotFoundError as exc:
        formatter.error(message=str(exc), error_code="NOT_INITIALIZED")
        raise typer.Exit(code=1) from None
    except ConfigError as exc:
        formatter.error(message=exc.message, error_code="CONFIG_ERROR")
        raise typer.Exit(code=5) from None
    except KeboolaApiError as exc:
        formatter.error(
            message=exc.message,
            error_code=exc.error_code,
        )
        raise typer.Exit(code=map_error_to_exit_code(exc)) from None

    if formatter.json_mode:
        formatter.output(result)
    else:
        _format_pull_result(formatter, result)


@sync_app.command("status")
def sync_status(
    ctx: typer.Context,
    directory: Path = typer.Option(
        Path("."),
        "--directory",
        "-d",
        help="Project root directory (must contain .keboola/)",
    ),
) -> None:
    """Show which local configurations have been modified, added, or deleted.

    Compares the local filesystem state against the manifest to detect
    changes since the last pull.
    """
    formatter = get_formatter(ctx)
    service = get_service(ctx, "sync_service")
    project_root = directory.resolve()

    try:
        result = service.status(project_root=project_root)
    except FileNotFoundError as exc:
        formatter.error(message=str(exc), error_code="NOT_INITIALIZED")
        raise typer.Exit(code=1) from None

    if formatter.json_mode:
        formatter.output(result)
    else:
        modified = result["modified"]
        added = result["added"]
        deleted = result["deleted"]
        unchanged = result["unchanged"]

        if not modified and not added and not deleted:
            formatter.console.print(
                f"[green]No changes detected.[/green] ({unchanged} configurations tracked)"
            )
            return

        if modified:
            formatter.console.print(f"\n[yellow]Modified ({len(modified)}):[/yellow]")
            for m in modified:
                formatter.console.print(f"  M {m['path']}")

        if added:
            formatter.console.print(f"\n[green]Added ({len(added)}):[/green]")
            for a in added:
                formatter.console.print(f"  A {a['path']}")

        if deleted:
            formatter.console.print(f"\n[red]Deleted ({len(deleted)}):[/red]")
            for d in deleted:
                formatter.console.print(f"  D {d['path']}")

        formatter.console.print(
            f"\n{len(modified)} modified, {len(added)} added, "
            f"{len(deleted)} deleted, {unchanged} unchanged"
        )


@sync_app.command("diff")
def sync_diff(
    ctx: typer.Context,
    project: str | None = typer.Option(
        None,
        "--project",
        help="Project alias to diff against",
    ),
    all_projects: bool = typer.Option(
        False,
        "--all-projects",
        help="Diff all configured projects in parallel",
    ),
    directory: Path = typer.Option(
        Path("."),
        "--directory",
        "-d",
        help="Project root directory (must contain .keboola/)",
    ),
) -> None:
    """Show detailed diff between local and remote configurations.

    Use --project for a single project or --all-projects for all configured
    projects in parallel.
    """
    formatter = get_formatter(ctx)
    service = get_service(ctx, "sync_service")

    if all_projects and project:
        formatter.error(
            message="Cannot use --project with --all-projects",
            error_code="USAGE_ERROR",
        )
        raise typer.Exit(code=2)
    if not all_projects and not project:
        formatter.error(
            message="Specify --project ALIAS or --all-projects",
            error_code="USAGE_ERROR",
        )
        raise typer.Exit(code=2)

    if all_projects:
        base_dir = directory.resolve()
        try:
            data = service.diff_all(base_dir)
        except ConfigError as exc:
            formatter.error(message=exc.message, error_code="CONFIG_ERROR")
            raise typer.Exit(code=5) from None

        if formatter.json_mode:
            formatter.output(data)
        else:
            _format_all_results(formatter, data, _format_diff_result)
        return

    project_root = directory.resolve()

    try:
        result = service.diff(alias=project, project_root=project_root)
    except FileNotFoundError as exc:
        formatter.error(message=str(exc), error_code="NOT_INITIALIZED")
        raise typer.Exit(code=1) from None
    except ConfigError as exc:
        formatter.error(message=exc.message, error_code="CONFIG_ERROR")
        raise typer.Exit(code=5) from None
    except KeboolaApiError as exc:
        formatter.error(message=exc.message, error_code=exc.error_code)
        raise typer.Exit(code=map_error_to_exit_code(exc)) from None

    if formatter.json_mode:
        formatter.output(result)
    else:
        changes = result["changes"]
        summary = result["summary"]
        remote_only = result.get("remote_only", [])

        if not changes and not remote_only:
            formatter.console.print(
                "[green]No differences found.[/green] Local and remote are in sync."
            )
            return

        # Categorize changes by direction
        prefix_map = {
            "added": "[green]+ ",
            "modified": "[yellow]~ ",
            "remote_modified": "[cyan]~ ",
            "conflict": "[red]! ",
            "deleted": "[red]- ",
        }
        suffix_map = {
            "added": "[/green]",
            "modified": "[/yellow]",
            "remote_modified": "[/cyan]",
            "conflict": "[/red]",
            "deleted": "[/red]",
        }
        label_map = {
            "added": "ADDED",
            "modified": "MODIFIED",
            "remote_modified": "REMOTE MODIFIED",
            "conflict": "CONFLICT",
            "deleted": "DELETED",
        }

        local_changes = [c for c in changes if c["change_type"] in ("added", "modified", "deleted")]
        remote_changes = [c for c in changes if c["change_type"] == "remote_modified"]
        conflict_changes = [c for c in changes if c["change_type"] == "conflict"]

        # Local changes (what push would do)
        if local_changes:
            formatter.console.print("[bold]Local changes (push would apply):[/bold]")
            for change in local_changes:
                ct = change["change_type"]
                label = _change_label(change)
                formatter.console.print(
                    f"  {prefix_map[ct]}{label_map[ct]} {label}{suffix_map[ct]}"
                )
                for detail in change.get("details", []):
                    formatter.console.print(f"    {detail}")
            formatter.console.print(
                f"\n{summary['added']} to create, {summary['modified']} to update, "
                f"{summary['deleted']} to delete"
            )

        # Remote changes (need pull)
        if remote_changes:
            if local_changes:
                formatter.console.print()
            formatter.console.print("[bold]Remote changes (run 'sync pull' to fetch):[/bold]")
            for change in remote_changes:
                label = _change_label(change)
                formatter.console.print(f"  [cyan]~ REMOTE MODIFIED {label}[/cyan]")
                for detail in change.get("details", []):
                    formatter.console.print(f"    {detail}")

        # Conflicts (both sides changed)
        if conflict_changes:
            if local_changes or remote_changes:
                formatter.console.print()
            formatter.console.print(
                "[bold red]Conflicts (both local and remote changed):[/bold red]"
            )
            for change in conflict_changes:
                label = _change_label(change)
                formatter.console.print(f"  [red]! CONFLICT {label}[/red]")
                for detail in change.get("details", []):
                    formatter.console.print(f"    {detail}")

        # Remote-only configs (new on server, not yet pulled)
        if remote_only:
            if changes:
                formatter.console.print()
            formatter.console.print(
                f"[bold]Remote only ({len(remote_only)} new, run 'sync pull' to fetch):[/bold]"
            )
            for cfg in remote_only:
                name = cfg.get("config_name", cfg.get("config_id", ""))
                formatter.console.print(f"  [cyan]+ NEW {cfg['component_id']}/{name}[/cyan]")


@sync_app.command("push")
def sync_push(
    ctx: typer.Context,
    project: str | None = typer.Option(
        None,
        "--project",
        help="Project alias to push changes to",
    ),
    all_projects: bool = typer.Option(
        False,
        "--all-projects",
        help="Push all configured projects in parallel",
    ),
    directory: Path = typer.Option(
        Path("."),
        "--directory",
        "-d",
        help="Project root directory (must contain .keboola/)",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would be pushed without actually pushing",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Allow deletion of remote configs that were removed locally",
    ),
) -> None:
    """Push local configuration changes to a Keboola project.

    Use --project for a single project or --all-projects for all configured
    projects in parallel.
    """
    formatter = get_formatter(ctx)
    service = get_service(ctx, "sync_service")

    if all_projects and project:
        formatter.error(
            message="Cannot use --project with --all-projects",
            error_code="USAGE_ERROR",
        )
        raise typer.Exit(code=2)
    if not all_projects and not project:
        formatter.error(
            message="Specify --project ALIAS or --all-projects",
            error_code="USAGE_ERROR",
        )
        raise typer.Exit(code=2)

    if all_projects:
        base_dir = directory.resolve()
        try:
            data = service.push_all(base_dir, dry_run=dry_run, force=force)
        except ConfigError as exc:
            formatter.error(message=exc.message, error_code="CONFIG_ERROR")
            raise typer.Exit(code=5) from None

        if formatter.json_mode:
            formatter.output(data)
        else:
            _format_all_results(formatter, data, _format_push_result)
        return

    project_root = directory.resolve()

    try:
        result = service.push(
            alias=project,
            project_root=project_root,
            dry_run=dry_run,
            force=force,
        )
    except FileNotFoundError as exc:
        formatter.error(message=str(exc), error_code="NOT_INITIALIZED")
        raise typer.Exit(code=1) from None
    except ConfigError as exc:
        formatter.error(message=exc.message, error_code="CONFIG_ERROR")
        raise typer.Exit(code=5) from None
    except KeboolaApiError as exc:
        formatter.error(message=exc.message, error_code=exc.error_code)
        raise typer.Exit(code=map_error_to_exit_code(exc)) from None

    if formatter.json_mode:
        formatter.output(result)
    else:
        status = result.get("status", "")

        if status == "no_changes":
            formatter.console.print("[green]No changes to push.[/green]")
            skipped_reason = result.get("skipped_reason")
            if skipped_reason:
                formatter.console.print(f"  [yellow]{skipped_reason}[/yellow]")
            return

        if status == "dry_run":
            formatter.console.print("[yellow]Dry run -- no changes applied:[/yellow]")
            for change in result.get("changes", []):
                label = _change_label(change)
                formatter.console.print(f"  {change['change_type'].upper()} {label}")
            summary = result["summary"]
            formatter.console.print(
                f"\nWould create {summary['added']}, update {summary['modified']}, "
                f"delete {summary['deleted']}"
            )
            return

        formatter.success(
            f"Pushed: {result['created']} created, "
            f"{result['updated']} updated, "
            f"{result['deleted']} deleted"
        )
        for change in result.get("pushed_details", []):
            label = _change_label(change)
            action = change["change_type"].upper()
            formatter.console.print(f"  {action} {label}")
        for err in result.get("errors", []):
            formatter.warning(
                f"  Error: {err['change_type']} {err['component_id']}/{err['config_id']}: "
                f"{err['message']}"
            )


@sync_app.command("branch-link")
def sync_branch_link(
    ctx: typer.Context,
    project: str = typer.Option(..., "--project", help="Project alias"),
    directory: Path = typer.Option(Path("."), "--directory", "-d", help="Project root directory"),
    branch_id: int | None = typer.Option(
        None, "--branch-id", help="Link to existing Keboola branch by ID"
    ),
    branch_name: str | None = typer.Option(
        None, "--branch-name", help="Create/find branch with this name"
    ),
) -> None:
    """Link the current git branch to a Keboola development branch.

    Creates a new Keboola dev branch if one doesn't exist with the same name
    as the current git branch.
    """
    formatter = get_formatter(ctx)
    service = get_service(ctx, "sync_service")
    project_root = directory.resolve()

    try:
        result = service.branch_link(
            alias=project,
            project_root=project_root,
            branch_id=branch_id,
            branch_name=branch_name,
        )
    except FileNotFoundError as exc:
        formatter.error(message=str(exc), error_code="NOT_INITIALIZED")
        raise typer.Exit(code=1) from None
    except ConfigError as exc:
        formatter.error(message=exc.message, error_code="CONFIG_ERROR")
        raise typer.Exit(code=5) from None
    except KeboolaApiError as exc:
        formatter.error(message=exc.message, error_code=exc.error_code)
        raise typer.Exit(code=map_error_to_exit_code(exc)) from None

    if formatter.json_mode:
        formatter.output(result)
    else:
        status = result["status"]
        if status == "already_linked":
            formatter.console.print(
                f"Already linked: {result['git_branch']} -> "
                f"Keboola branch {result['keboola_branch_id']} ({result['keboola_branch_name']})"
            )
        else:
            formatter.success(
                f"Linked {result['git_branch']} -> "
                f"Keboola branch {result['keboola_branch_id']} ({result['keboola_branch_name']})"
            )


@sync_app.command("branch-unlink")
def sync_branch_unlink(
    ctx: typer.Context,
    directory: Path = typer.Option(Path("."), "--directory", "-d", help="Project root directory"),
) -> None:
    """Remove the branch mapping for the current git branch.

    Does NOT delete the Keboola branch itself.
    """
    formatter = get_formatter(ctx)
    service = get_service(ctx, "sync_service")
    project_root = directory.resolve()

    try:
        result = service.branch_unlink(project_root=project_root)
    except FileNotFoundError as exc:
        formatter.error(message=str(exc), error_code="NOT_INITIALIZED")
        raise typer.Exit(code=1) from None
    except ConfigError as exc:
        formatter.error(message=exc.message, error_code="CONFIG_ERROR")
        raise typer.Exit(code=5) from None

    if formatter.json_mode:
        formatter.output(result)
    else:
        if result["status"] == "not_linked":
            formatter.console.print(f"Branch '{result['git_branch']}' is not linked.")
        else:
            formatter.success(
                f"Unlinked {result['git_branch']} from Keboola branch {result['keboola_branch_id']}"
            )


@sync_app.command("branch-status")
def sync_branch_status(
    ctx: typer.Context,
    directory: Path = typer.Option(Path("."), "--directory", "-d", help="Project root directory"),
) -> None:
    """Show the branch mapping status for the current git branch."""
    formatter = get_formatter(ctx)
    service = get_service(ctx, "sync_service")
    project_root = directory.resolve()

    try:
        result = service.branch_status(project_root=project_root)
    except FileNotFoundError as exc:
        formatter.error(message=str(exc), error_code="NOT_INITIALIZED")
        raise typer.Exit(code=1) from None

    if formatter.json_mode:
        formatter.output(result)
    else:
        if not result.get("git_branching"):
            formatter.console.print("Git-branching mode is not enabled.")
            return

        git_branch = result.get("git_branch", "unknown")
        if result.get("linked"):
            if result.get("is_production"):
                formatter.console.print(
                    f"Branch: {git_branch}\nKeboola: production\nStatus: [green]Linked[/green]"
                )
            else:
                formatter.console.print(
                    f"Branch: {git_branch}\n"
                    f"Keboola: {result['keboola_branch_id']} ({result['keboola_branch_name']})\n"
                    f"Status: [green]Linked[/green]"
                )
        else:
            formatter.console.print(
                f"Branch: {git_branch}\n"
                f"Keboola: (none)\n"
                f"Status: [red]Not linked[/red]\n\n"
                f"Run 'kbagent sync branch-link --project ALIAS' to link."
            )
