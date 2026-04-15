"""Lineage commands - column-level dependency analysis across projects.

Thin CLI layer: parses arguments, calls DeepLineageService, formats output.
No business logic belongs here.

Two subcommands:
  build -- scan sync'd projects, build lineage graph, save cache
  show  -- query upstream/downstream from cached graph
"""

import json
import re
from pathlib import Path

import typer

from ._helpers import (
    check_cli_permission,
    emit_hint,
    get_formatter,
    get_service,
    should_hint,
)

lineage_app = typer.Typer(
    help="Column-level data lineage across projects.\n\n"
    "Build a dependency graph from sync'd data, then query upstream/downstream."
)


@lineage_app.callback(invoke_without_command=True)
def _lineage_callback(ctx: typer.Context) -> None:
    check_cli_permission(ctx, "lineage")
    if ctx.invoked_subcommand is None:
        # No subcommand -> show help
        click_cmd = typer.main.get_command(lineage_app)
        ctx_help = click_cmd.make_context("lineage", [])
        typer.echo(click_cmd.get_help(ctx_help))


# -- lineage build ---------------------------------------------------------


@lineage_app.command("build")
def lineage_build(
    ctx: typer.Context,
    directory: Path = typer.Option(
        Path("."),
        "--directory",
        "-d",
        help="Root directory with sync'd projects (default: current directory).",
    ),
    output: Path = typer.Option(
        ...,
        "--output",
        "-o",
        help="Output JSON file for the lineage graph (required).",
    ),
    ai: bool = typer.Option(
        False,
        "--ai",
        help="Generate AI task file for SQL/Python analysis (2-step: AI processes, then re-build).",
    ),
    refresh: bool = typer.Option(
        False,
        "--refresh",
        help="Sync pull all projects first, then rebuild.",
    ),
) -> None:
    """Build column-level lineage graph from sync'd data.

    Scans all sync'd projects (from `sync pull --all-projects`), detects
    table dependencies via config mappings and SQL parsing, and saves the
    graph to a JSON cache file for fast queries with `lineage show`.

    AI-enhanced analysis is a 2-step process:

      1. kbagent lineage build -d /path -o lineage.json --ai
         (builds deterministic graph + generates .lineage_ai_tasks.json)

      2. AI agent reads the task file, analyzes each code file from disk,
         writes results to .lineage_ai_results.json

      3. kbagent lineage build -d /path -o lineage.json
         (automatically applies AI results if .lineage_ai_results.json exists)
    """
    if should_hint(ctx):
        emit_hint(
            ctx,
            "lineage.build",
            directory=str(directory),
            ai=ai,
        )
        return

    formatter = get_formatter(ctx)
    service = get_service(ctx, "deep_lineage_service")

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

    result = service.build_lineage(root, generate_ai_tasks=ai)

    try:
        with open(output, "w") as f:
            json.dump(result, f, indent=2)
    except OSError as exc:
        formatter.error(
            message=f"Cannot write output file '{output}': {exc}",
            error_code="WRITE_ERROR",
        )
        raise typer.Exit(code=1) from None

    if formatter.json_mode:
        formatter.output(result)
    else:
        summary = result.get("summary", {})
        formatter.console.print("\n[bold]Lineage graph built[/bold]")
        formatter.console.print(f"  Tables: {summary.get('tables', 0)}")
        formatter.console.print(f"  Configurations: {summary.get('configurations', 0)}")
        formatter.console.print(f"  Edges: {summary.get('edges', 0)}")
        if summary.get("detection_methods"):
            formatter.console.print("\n  Detection methods:")
            for k, v in sorted(summary["detection_methods"].items(), key=lambda x: -x[1]):
                formatter.console.print(f"    {k}: {v}")
        # AI status
        ai_status = result.get("ai_status", {})
        if ai_status.get("ai_results_applied"):
            formatter.console.print(
                f"\n  AI results applied: {ai_status.get('ai_edges_added', 0)} edges added"
            )
        if ai_status.get("ai_tasks_generated"):
            formatter.console.print(
                f"\n  [bold]AI tasks generated: {ai_status['ai_tasks_generated']}[/bold]"
                f" (already done: {ai_status.get('ai_already_done', 0)})"
            )
            formatter.console.print(f"  Task file: {ai_status['ai_tasks_file']}")
            formatter.console.print(
                "  Next: let your AI agent process the tasks, then re-run this command."
            )
        formatter.console.print(f"\n  Saved to: {output}")


# -- lineage show ----------------------------------------------------------


@lineage_app.command("show")
def lineage_show(
    ctx: typer.Context,
    load: Path = typer.Option(
        ...,
        "--load",
        "-l",
        help="Lineage JSON cache file (from `lineage build`).",
    ),
    upstream: str | None = typer.Option(
        None,
        "--upstream",
        help="Show upstream dependencies. Use 'project:table_id' or just 'table_id'.",
    ),
    downstream: str | None = typer.Option(
        None,
        "--downstream",
        help="Show downstream dependents. Use 'project:table_id' or just 'table_id'.",
    ),
    column: str | None = typer.Option(
        None,
        "--column",
        "-c",
        help="Trace a specific column (use with --upstream/--downstream).",
    ),
    columns: bool = typer.Option(
        False,
        "--columns",
        help="Show column-level mapping detail on edges.",
    ),
    project: str | None = typer.Option(
        None,
        "--project",
        "-p",
        help="Project alias filter for queries.",
    ),
    depth: int = typer.Option(10, "--depth", help="Max traversal depth (default: 10)."),
    format: str = typer.Option(
        "text",
        "--format",
        "-f",
        help="Output format: text, mermaid, or html.",
    ),
) -> None:
    """Query upstream/downstream dependencies from a cached lineage graph.

    Requires a lineage cache file built with `lineage build`.

    Node identifiers for --upstream/--downstream:

      Full FQN:    project-alias:bucket_id.table_name

      Table only:  bucket_id.table_name  (auto-resolves, warns if ambiguous)

    Output formats (--format):

      text     Rich tree (default)

      mermaid  Mermaid flowchart source code

      html     Self-contained HTML file with embedded mermaid diagram

    Examples:

      kbagent lineage show -l lineage.json --downstream "project:table"

      kbagent lineage show -l lineage.json --upstream "project:table" --columns

      kbagent lineage show -l lineage.json --upstream "project:table" -c "col_name"

      kbagent lineage show -l lineage.json --downstream "project:table" -f mermaid

      kbagent lineage show -l lineage.json --downstream "project:table" -f html
    """
    if should_hint(ctx):
        emit_hint(
            ctx,
            "lineage.show",
            upstream=upstream,
            downstream=downstream,
            project=project,
            depth=depth,
        )
        return

    formatter = get_formatter(ctx)
    service = get_service(ctx, "deep_lineage_service")

    valid_formats = ("text", "mermaid", "html")
    if format not in valid_formats:
        formatter.error(
            message=f"Invalid format '{format}'. Must be one of: {', '.join(valid_formats)}",
            error_code="INVALID_FORMAT",
        )
        raise typer.Exit(code=2)

    if not load.exists():
        formatter.error(message=f"Cache file not found: {load}", error_code="FILE_NOT_FOUND")
        raise typer.Exit(code=1)

    graph = service.load_from_cache(load)

    if not upstream and not downstream:
        # No query -> show summary
        if formatter.json_mode:
            formatter.output(graph.to_dict())
        else:
            summary = graph.summary()
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
            # Show example table FQNs so the user knows what to query
            table_fqns = sorted(graph.tables.keys()) if graph.tables else []
            if table_fqns:
                show_limit = 10
                formatter.console.print("\n  Example tables (use with --upstream or --downstream):")
                for fqn in table_fqns[:show_limit]:
                    formatter.console.print(f"    {fqn}")
                remaining = len(table_fqns) - show_limit
                if remaining > 0:
                    formatter.console.print(f"    ... and {remaining} more")
        return

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
        elif format in ("mermaid", "html"):
            _output_mermaid_or_html(formatter, service, graph, query_result, "upstream", format)
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
        elif format in ("mermaid", "html"):
            _output_mermaid_or_html(formatter, service, graph, query_result, "downstream", format)
        else:
            _format_lineage_tree(formatter, graph, query_result, "downstream", **display_opts)


# -- Output formatting helpers ----------------------------------------------


def _output_mermaid_or_html(
    formatter,
    service,
    graph,
    query_result: dict,
    direction: str,
    output_format: str,
) -> None:
    """Render query result as mermaid or HTML and output it."""
    from ..services.deep_lineage_service import DeepLineageService

    node_fqn = query_result["node"]
    edges = query_result.get("edges", [])

    mermaid_code = DeepLineageService.render_mermaid(edges, graph, direction, node_fqn)

    if output_format == "mermaid":
        typer.echo(mermaid_code)
    elif output_format == "html":
        title = f"Lineage {direction} of {node_fqn}"
        html_content = DeepLineageService.render_html(mermaid_code, title)
        sanitized_node = re.sub(r"[^a-zA-Z0-9_]", "_", node_fqn)
        filename = f"lineage_{direction}_{sanitized_node}.html"
        try:
            with open(filename, "w") as f:
                f.write(html_content)
            if not formatter.json_mode:
                formatter.console.print(f"HTML lineage diagram saved to: {filename}")
        except OSError as exc:
            formatter.error(
                message=f"Cannot write HTML file '{filename}': {exc}",
                error_code="WRITE_ERROR",
            )
            raise typer.Exit(code=1) from None


def _filter_column_json(result: dict, column_name: str) -> dict:
    """Filter JSON query result to only edges relevant to a specific column."""
    filtered_edges = []
    col_lower = column_name.lower()
    for edge in result.get("edges", []):
        col_map = edge.get("column_mapping", {})
        edge_columns = edge.get("columns", [])
        mapped_keys = [k for k in col_map if k.lower() == col_lower]
        mapped_vals = [k for k, v in col_map.items() if v.lower().endswith(f".{col_lower}")]
        col_match = any(c.lower() == col_lower for c in edge_columns)
        if mapped_keys or mapped_vals or col_match:
            relevant_map = {
                k: v for k, v in col_map.items() if k in mapped_keys or k in mapped_vals
            }
            edge_copy = dict(edge)
            if relevant_map:
                edge_copy["column_mapping"] = relevant_map
            filtered_edges.append(edge_copy)
        elif not col_map and not edge_columns:
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
        edge_columns = edge.get("columns", [])

        if col_lower:
            has_in_map = any(
                k.lower() == col_lower or v.lower().endswith(f".{col_lower}")
                for k, v in col_map.items()
            )
            has_in_cols = any(c.lower() == col_lower for c in edge_columns)
            is_structural = not col_map and not edge_columns
            if not has_in_map and not has_in_cols and not is_structural:
                continue

        indent = "  " * edge["depth"]
        target_fqn = edge["source"] if direction == "upstream" else edge["target"]

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

        col_hint = ""
        if not show_columns and edge_columns:
            col_list = edge_columns[:5]
            suffix = f"... +{len(edge_columns) - 5}" if len(edge_columns) > 5 else ""
            col_hint = f" [{', '.join(col_list)}{suffix}]"

        formatter.console.print(f"{indent}{arrow} ({edge['detection']}) {node_desc}{col_hint}")

        if show_columns and col_map:
            map_indent = "  " * (edge["depth"] + 1)
            items = list(col_map.items())
            if col_lower:
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
        elif show_columns and edge_columns:
            map_indent = "  " * (edge["depth"] + 1)
            show_cols = edge_columns
            if col_lower:
                show_cols = [c for c in edge_columns if c.lower() == col_lower]
            for c in show_cols[:10]:
                formatter.console.print(f"{map_indent}[dim]{c}[/dim]")
            if len(show_cols) > 10:
                formatter.console.print(f"{map_indent}[dim]... +{len(show_cols) - 10} more[/dim]")
