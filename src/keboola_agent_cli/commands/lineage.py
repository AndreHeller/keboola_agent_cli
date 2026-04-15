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
        help="Show upstream dependencies. Use full FQN 'project:table_id' or just 'table_id'.",
    ),
    downstream: str | None = typer.Option(
        None,
        "--downstream",
        help="Show downstream dependents. Use full FQN 'project:table_id' or just 'table_id'.",
    ),
    column: str | None = typer.Option(
        None,
        "--column",
        "-c",
        help="Trace a specific column through the lineage (use with --upstream/--downstream).",
    ),
    columns: bool = typer.Option(
        False,
        "--columns",
        help="Show column-level mapping detail on edges (AI-detected).",
    ),
    project: str | None = typer.Option(
        None,
        "--project",
        "-p",
        help="Project alias filter for queries.",
    ),
    depth: int = typer.Option(10, "--depth", help="Max traversal depth (default: 10)"),
    ai: bool = typer.Option(False, "--ai", help="Enable AI analysis of SQL/Python code"),
    ai_model: str = typer.Option(
        "haiku",
        "--ai-model",
        help="AI model for analysis: haiku (fast/cheap) or sonnet (better quality)",
    ),
    ai_workers: int = typer.Option(4, "--ai-workers", help="Parallel AI workers (default: 4)"),
    refresh: bool = typer.Option(
        False,
        "--refresh",
        help="Sync pull all projects first, then rebuild lineage. One-command update.",
    ),
) -> None:
    """Column-level lineage from sync'd data on disk.

    Scans all sync'd projects (from `sync pull --all-projects`), builds a
    comprehensive dependency graph, and supports upstream/downstream queries.

    Node identifiers for --upstream/--downstream:

      Full FQN:   project-alias:bucket_id.table_name

      Table only:  bucket_id.table_name  (auto-resolves, warns if ambiguous)

    Workflow:

      1. Build + cache:   kbagent lineage deep -d /path -o lineage.json

      2. Query:           kbagent lineage deep -l lineage.json --downstream "project:table"

      3. Column detail:   kbagent lineage deep -l lineage.json --upstream "project:table" --columns

      4. Trace a column:  kbagent lineage deep -l lineage.json --upstream "project:table" -c "col_name"

      5. With AI:         kbagent lineage deep -d /path -o lineage.json --ai

      6. Update:          kbagent lineage deep -d /path -o lineage.json --refresh --ai
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

        # --refresh: sync pull all projects first
        if refresh:
            if not formatter.json_mode:
                formatter.console.print("[bold]Syncing all projects...[/bold]")
            sync_service = get_service(ctx, "sync_service")
            sync_result = sync_service.pull_all(base_dir=root)
            summary = sync_result.get("summary", {})
            if not formatter.json_mode:
                formatter.console.print(
                    f"  Synced {summary.get('success', 0)}/{summary.get('total', 0)} projects"
                    f" ({summary.get('failed', 0)} failed)\n"
                )

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
    display_opts = {"show_columns": columns, "filter_column": column}

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
            if column:
                query_result = _filter_column_json(query_result, column)
            formatter.output(query_result)
        else:
            _format_lineage_tree(formatter, graph, query_result, "upstream", **display_opts)

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
            if column:
                query_result = _filter_column_json(query_result, column)
            formatter.output(query_result)
        else:
            _format_lineage_tree(formatter, graph, query_result, "downstream", **display_opts)

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


def _filter_column_json(result: dict, column_name: str) -> dict:
    """Filter JSON query result to only edges relevant to a specific column."""
    filtered_edges = []
    col_lower = column_name.lower()
    for edge in result.get("edges", []):
        col_map = edge.get("column_mapping", {})
        columns = edge.get("columns", [])
        # Match: column is in the mapping keys/values, or in the columns list
        mapped_keys = [k for k in col_map if k.lower() == col_lower]
        mapped_vals = [k for k, v in col_map.items() if v.lower().endswith(f".{col_lower}")]
        col_match = any(c.lower() == col_lower for c in columns)
        if mapped_keys or mapped_vals or col_match:
            # Keep only the relevant mappings
            relevant_map = {
                k: v for k, v in col_map.items() if k in mapped_keys or k in mapped_vals
            }
            edge_copy = dict(edge)
            if relevant_map:
                edge_copy["column_mapping"] = relevant_map
            filtered_edges.append(edge_copy)
        elif not col_map and not columns:
            # Keep structural edges (output_mapping etc.) that have no column info
            filtered_edges.append(edge)
    result_copy = dict(result)
    result_copy["edges"] = filtered_edges
    result_copy["column_filter"] = column_name
    return result_copy


def _format_lineage_tree(
    formatter,
    graph,
    result: dict,
    direction: str,
    show_columns: bool = False,
    filter_column: str | None = None,
) -> None:
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
        n_cols = node_info.get("columns", 0)
        rows = node_info.get("rows", 0)
        desc = f"[table] {node_fqn} ({n_cols} cols, {rows:,} rows)"
    elif node_info.get("name"):
        desc = f"[{node_type}] {node_info['name']} ({node_info.get('component', '')})"
    else:
        desc = f"[{node_type}] {node_fqn}"

    header = f"\n[bold]{label} of {desc}[/bold]"
    if filter_column:
        header += f"  [dim](column: {filter_column})[/dim]"
    formatter.console.print(header + "\n")

    if not edges:
        formatter.console.print("  (none found)")
        return

    col_lower = filter_column.lower() if filter_column else None

    for edge in sorted(edges, key=lambda e: e["depth"]):
        col_map = edge.get("column_mapping", {})
        columns = edge.get("columns", [])

        # When filtering by column, skip edges that don't mention it
        if col_lower:
            has_in_map = any(
                k.lower() == col_lower or v.lower().endswith(f".{col_lower}")
                for k, v in col_map.items()
            )
            has_in_cols = any(c.lower() == col_lower for c in columns)
            is_structural = not col_map and not columns
            if not has_in_map and not has_in_cols and not is_structural:
                continue

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

        # Show column list summary (unless --columns expands them)
        col_hint = ""
        if not show_columns and columns:
            col_list = columns[:5]
            suffix = f"... +{len(columns) - 5}" if len(columns) > 5 else ""
            col_hint = f" [{', '.join(col_list)}{suffix}]"

        formatter.console.print(f"{indent}{arrow} ({edge['detection']}) {node_desc}{col_hint}")

        # --columns: show full column mapping
        if show_columns and col_map:
            map_indent = "  " * (edge["depth"] + 1)
            items = list(col_map.items())
            if col_lower:
                # Only show mappings for the filtered column
                items = [
                    (k, v)
                    for k, v in items
                    if k.lower() == col_lower or v.lower().endswith(f".{col_lower}")
                ]
            for out_col, src_expr in items:
                src_short = src_expr.split(".")[-1] if "." in src_expr else src_expr
                src_table = ".".join(src_expr.split(".")[:-1]) if "." in src_expr else ""
                if src_table:
                    formatter.console.print(
                        f"{map_indent}[dim]{out_col}[/dim] <- {src_table}.[bold]{src_short}[/bold]"
                    )
                else:
                    formatter.console.print(f"{map_indent}[dim]{out_col}[/dim] <- {src_expr}")
        elif show_columns and columns:
            # No AI mapping but we have column list from input mapping
            map_indent = "  " * (edge["depth"] + 1)
            show_cols = columns
            if col_lower:
                show_cols = [c for c in columns if c.lower() == col_lower]
            for c in show_cols[:10]:
                formatter.console.print(f"{map_indent}[dim]{c}[/dim]")
            if len(show_cols) > 10:
                formatter.console.print(f"{map_indent}[dim]... +{len(show_cols) - 10} more[/dim]")


@lineage_app.callback(invoke_without_command=True)
def lineage_callback(ctx: typer.Context) -> None:
    """Default to 'show' when no subcommand is given."""
    if ctx.invoked_subcommand is None:
        ctx.invoke(lineage_show, ctx=ctx, project=None)
