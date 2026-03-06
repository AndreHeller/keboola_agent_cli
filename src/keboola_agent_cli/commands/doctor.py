"""Doctor command - thin CLI wrapper over DoctorService.

Delegates all health check logic to DoctorService (3-layer architecture).
"""

import typer
from rich.console import Console

from ..output import format_doctor_panel
from ._helpers import get_formatter, get_service


def doctor_command(
    ctx: typer.Context,
    fix: bool = typer.Option(
        False,
        "--fix",
        help="Auto-fix issues: install MCP server binary for faster startup.",
    ),
) -> None:
    """Run health checks on CLI configuration and project connectivity."""
    formatter = get_formatter(ctx)
    doctor_service = get_service(ctx, "doctor_service")

    # If --fix, run warmup before checks
    if fix:
        console = Console()
        console.print("[bold]Running auto-fix...[/bold]")
        warmup_result = doctor_service.warmup()
        if warmup_result["installed"]:
            console.print(f"[green]OK[/green] {warmup_result['message']}")
        else:
            console.print(f"[dim]{warmup_result['message']}[/dim]")
        console.print()

    result = doctor_service.run_checks()
    formatter.output(result, format_doctor_panel)
