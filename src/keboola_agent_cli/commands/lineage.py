"""Lineage commands - analyze cross-project data flow via bucket sharing.

Thin CLI layer: parses arguments, calls LineageService, formats output.
No business logic belongs here.
"""

import json
from pathlib import Path

import typer

from ..errors import ConfigError
from ..output import format_lineage_table
from ._helpers import (
    check_cli_permission,
    emit_hint,
    emit_project_warnings,
    get_formatter,
    get_service,
    should_hint,
)

lineage_app = typer.Typer(help="Analyze cross-project data lineage via bucket sharing")


@lineage_app.callback(invoke_without_command=True)
def _lineage_permission_check(ctx: typer.Context) -> None:
    check_cli_permission(ctx, "lineage")


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
    if should_hint(ctx):
        emit_hint(ctx, "lineage.show", project=project)
        return
    formatter = get_formatter(ctx)
    service = get_service(ctx, "lineage_service")

    try:
        result = service.get_lineage(aliases=project)
    except ConfigError as exc:
        formatter.error(message=exc.message, error_code="CONFIG_ERROR")
        raise typer.Exit(code=5) from None

    if formatter.json_mode:
        formatter.output(result)
    else:
        format_lineage_table(formatter.console, result)
        emit_project_warnings(formatter, result)


@lineage_app.command("deep")
def lineage_deep(
    ctx: typer.Context,
    directory: Path = typer.Option(
        Path("."),
        "--directory",
        "-d",
        help="Root directory with sync'd projects (default: current directory)",
    ),
    output: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Save lineage graph to JSON file (cache for fast queries)",
    ),
    load: Path | None = typer.Option(
        None,
        "--load",
        "-l",
        help="Load from cached lineage JSON (skip scan)",
    ),
    upstream: str | None = typer.Option(
        None,
        "--upstream",
        help="Show upstream dependencies of a table or config",
    ),
    downstream: str | None = typer.Option(
        None,
        "--downstream",
        help="Show downstream dependents of a table or config",
    ),
    project: str | None = typer.Option(
        None,
        "--project",
        "-p",
        help="Project alias filter for queries",
    ),
    depth: int = typer.Option(10, "--depth", help="Max traversal depth (default: 10)"),
    ai: bool = typer.Option(False, "--ai", help="Enable AI analysis of SQL/Python code"),
    ai_model: str = typer.Option(
        "haiku",
        "--ai-model",
        help="AI model for analysis: haiku (fast/cheap) or sonnet (better quality)",
    ),
    ai_workers: int = typer.Option(4, "--ai-workers", help="Parallel AI workers (default: 4)"),
) -> None:
    """Column-level lineage from sync'd data on disk.

    Scans all sync'd projects (from `sync pull --all-projects`), builds a
    comprehensive dependency graph, and supports upstream/downstream queries.

    Workflow:
      1. Build and cache:  kbagent lineage deep -d /path --output lineage.json
      2. Fast query:       kbagent lineage deep --load lineage.json --downstream "table_id"
      3. With AI:          kbagent lineage deep -d /path -o lineage.json --ai
    """
    formatter = get_formatter(ctx)
    service = get_service(ctx, "deep_lineage_service")

    # Build or load graph
    if load:
        if not load.exists():
            formatter.error(message=f"Cache file not found: {load}", error_code="FILE_NOT_FOUND")
            raise typer.Exit(code=1)
        graph = service.load_from_cache(load)
    else:
        root = directory.resolve()
        if not root.is_dir():
            formatter.error(message=f"Directory not found: {root}", error_code="DIR_NOT_FOUND")
            raise typer.Exit(code=1)

        result = service.build_lineage(
            root,
            include_ai=ai,
            ai_model=ai_model,
            ai_workers=ai_workers,
        )

        if output:
            with open(output, "w") as f:
                json.dump(result, f, indent=2)

        if formatter.json_mode and not upstream and not downstream:
            formatter.output(result)
            return

        graph = service._graph_from_dict(result)

    # Query mode
    if upstream:
        query_result = service.query_upstream(graph, upstream, project or "", depth)
        if "error" in query_result:
            suggestions = query_result.get("suggestions", [])
            msg = query_result["error"]
            if suggestions:
                msg += "\nDid you mean: " + ", ".join(suggestions[:5])
            formatter.error(message=msg, error_code="NODE_NOT_FOUND")
            raise typer.Exit(code=1)

        if formatter.json_mode:
            formatter.output(query_result)
        else:
            _format_lineage_tree(formatter, graph, query_result, "upstream")

    if downstream:
        query_result = service.query_downstream(graph, downstream, project or "", depth)
        if "error" in query_result:
            suggestions = query_result.get("suggestions", [])
            msg = query_result["error"]
            if suggestions:
                msg += "\nDid you mean: " + ", ".join(suggestions[:5])
            formatter.error(message=msg, error_code="NODE_NOT_FOUND")
            raise typer.Exit(code=1)

        if formatter.json_mode:
            formatter.output(query_result)
        else:
            _format_lineage_tree(formatter, graph, query_result, "downstream")

    # If no query, show summary
    if not upstream and not downstream and not formatter.json_mode:
        summary = graph.summary() if hasattr(graph, "summary") else {}
        formatter.console.print("\n[bold]Lineage Graph Summary[/bold]")
        formatter.console.print(f"  Tables: {summary.get('tables', 0)}")
        formatter.console.print(f"  Configurations: {summary.get('configurations', 0)}")
        formatter.console.print(f"  Edges: {summary.get('edges', 0)}")
        if summary.get("edge_types"):
            formatter.console.print("\n  Edge types:")
            for k, v in sorted(summary["edge_types"].items(), key=lambda x: -x[1]):
                formatter.console.print(f"    {k}: {v}")
        if summary.get("detection_methods"):
            formatter.console.print("\n  Detection methods:")
            for k, v in sorted(summary["detection_methods"].items(), key=lambda x: -x[1]):
                formatter.console.print(f"    {k}: {v}")
        if output:
            formatter.console.print(f"\n  Saved to: {output}")


def _format_lineage_tree(formatter, graph, result: dict, direction: str) -> None:
    """Format lineage query result as a human-readable tree."""
    from ..services.deep_lineage_service import LineageGraph

    node_fqn = result["node"]
    node_info = result.get("node_info", {})
    edges = result.get("edges", [])

    arrow = "<-" if direction == "upstream" else "->"
    label = "Upstream dependencies" if direction == "upstream" else "Downstream dependents"

    # Describe the root node
    node_type = node_info.get("type", "unknown")
    if node_type == "table":
        cols = node_info.get("columns", 0)
        rows = node_info.get("rows", 0)
        desc = f"[table] {node_fqn} ({cols} cols, {rows:,} rows)"
    elif node_info.get("name"):
        desc = f"[{node_type}] {node_info['name']} ({node_info.get('component', '')})"
    else:
        desc = f"[{node_type}] {node_fqn}"

    formatter.console.print(f"\n[bold]{label} of {desc}[/bold]\n")

    if not edges:
        formatter.console.print("  (none found)")
        return

    for edge in sorted(edges, key=lambda e: e["depth"]):
        indent = "  " * edge["depth"]
        target_fqn = edge["source"] if direction == "upstream" else edge["target"]

        # Describe the linked node
        if isinstance(graph, LineageGraph):
            if target_fqn in graph.tables:
                t = graph.tables[target_fqn]
                node_desc = f"[table] {target_fqn} ({len(t.columns)} cols, {t.rows_count:,} rows)"
            elif target_fqn in graph.configurations:
                c = graph.configurations[target_fqn]
                node_desc = (
                    f"[{c.component_type}] {c.project_alias}:{c.config_name} ({c.component_id})"
                )
            else:
                node_desc = target_fqn
        else:
            node_desc = target_fqn

        cols = ""
        if edge.get("columns"):
            col_list = edge["columns"][:5]
            suffix = f"... +{len(edge['columns']) - 5}" if len(edge["columns"]) > 5 else ""
            cols = f" [{', '.join(col_list)}{suffix}]"

        formatter.console.print(f"{indent}{arrow} ({edge['detection']}) {node_desc}{cols}")

        # Show column-level mapping if available
        col_map = edge.get("column_mapping", {})
        if col_map:
            map_indent = "  " * (edge["depth"] + 1)
            items = list(col_map.items())
            for out_col, src_expr in items[:6]:
                # Shorten source expression for readability
                src_short = src_expr.split(".")[-1] if "." in src_expr else src_expr
                src_table = ".".join(src_expr.split(".")[:-1]) if "." in src_expr else ""
                if src_table:
                    formatter.console.print(
                        f"{map_indent}[dim]{out_col}[/dim] <- {src_table}.[bold]{src_short}[/bold]"
                    )
                else:
                    formatter.console.print(f"{map_indent}[dim]{out_col}[/dim] <- {src_expr}")
            if len(items) > 6:
                formatter.console.print(
                    f"{map_indent}[dim]... +{len(items) - 6} more columns[/dim]"
                )


@lineage_app.callback(invoke_without_command=True)
def lineage_callback(ctx: typer.Context) -> None:
    """Default to 'show' when no subcommand is given."""
    if ctx.invoked_subcommand is None:
        ctx.invoke(lineage_show, ctx=ctx, project=None)
