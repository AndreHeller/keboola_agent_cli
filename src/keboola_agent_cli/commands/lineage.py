"""Lineage commands - column-level dependency analysis across projects.

Thin CLI layer: parses arguments, calls DeepLineageService, formats output.
No business logic belongs here.

Four subcommands:
  build -- scan sync'd projects, build lineage graph, save cache
  show  -- query upstream/downstream from cached graph
  serve -- start local web server with interactive D3.js visualization
"""

import http.server
import json
import re
import threading
import webbrowser
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


# -- lineage serve ---------------------------------------------------------

_LINEAGE_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Keboola Lineage Graph</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  background: #1a1a2e; color: #e0e0e0;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  overflow: hidden; height: 100vh;
}
#controls {
  position: fixed; top: 0; left: 0; right: 0; z-index: 10;
  background: rgba(26, 26, 46, 0.95); padding: 10px 16px;
  display: flex; align-items: center; gap: 12px;
  border-bottom: 1px solid #333;
}
#controls h1 { font-size: 16px; font-weight: 600; white-space: nowrap; }
#search {
  padding: 6px 12px; border-radius: 6px; border: 1px solid #444;
  background: #16213e; color: #e0e0e0; font-size: 14px; width: 300px;
  outline: none;
}
#search:focus { border-color: #4fc3f7; }
#stats { font-size: 12px; color: #888; white-space: nowrap; }
#legend {
  position: fixed; bottom: 16px; left: 16px; z-index: 10;
  background: rgba(26, 26, 46, 0.92); padding: 12px 16px;
  border-radius: 8px; border: 1px solid #333; font-size: 12px;
}
#legend div { margin: 4px 0; display: flex; align-items: center; gap: 8px; }
.legend-swatch {
  width: 14px; height: 14px; display: inline-block; border-radius: 2px;
}
.legend-circle { border-radius: 50%; }
#tooltip {
  position: fixed; display: none; z-index: 20;
  background: rgba(22, 33, 62, 0.96); border: 1px solid #4fc3f7;
  border-radius: 8px; padding: 10px 14px; font-size: 13px;
  max-width: 380px; pointer-events: none;
}
#tooltip .tt-title { font-weight: 600; margin-bottom: 4px; }
#tooltip .tt-row { color: #aaa; margin: 2px 0; }
svg { width: 100vw; height: 100vh; }
.link { stroke-opacity: 0.4; fill: none; }
.link-label { font-size: 9px; fill: #777; pointer-events: none; }
.node-label {
  font-size: 10px; fill: #ccc; pointer-events: none;
  text-anchor: middle; dominant-baseline: central;
}
.node { cursor: pointer; stroke-width: 1.5; }
.node:hover { stroke-width: 3; }
marker { overflow: visible; }
</style>
</head>
<body>
<div id="controls">
  <h1>Lineage Graph</h1>
  <input id="search" type="text" placeholder="Search nodes..." autocomplete="off">
  <span id="stats"></span>
</div>
<div id="legend">
  <div><span class="legend-swatch legend-circle" style="background:#4fc3f7"></span> Table</div>
  <div><span class="legend-swatch" style="background:#81c784"></span> Configuration</div>
  <div>
    <span class="legend-swatch"
          style="background:#ff8a65;height:2px;width:20px;border-radius:0"></span>
    Cross-project edge
  </div>
  <div style="color:#aaa">Click node: highlight upstream (red) / downstream (blue)</div>
  <div style="color:#aaa">Scroll: zoom | Drag: pan</div>
</div>
<div id="tooltip">
  <div class="tt-title"></div>
  <div class="tt-body"></div>
</div>
<svg></svg>
<script src="https://d3js.org/d3.v7.min.js"></script>
<script>
(function() {
  var WIDTH = window.innerWidth;
  var HEIGHT = window.innerHeight;

  fetch("/data.json").then(function(r) { return r.json(); }).then(function(raw) {
    var graph = buildGraph(raw);
    renderGraph(graph);
  });

  function buildGraph(raw) {
    var nodeMap = {};
    var links = [];
    var edges = raw.edges || [];

    function ensureNode(fqn, type, meta) {
      if (!nodeMap[fqn]) {
        nodeMap[fqn] = { id: fqn, type: type || "unknown", meta: meta || {}, conns: 0 };
      } else if (meta) {
        Object.assign(nodeMap[fqn].meta, meta);
        if (type) nodeMap[fqn].type = type;
      }
      return nodeMap[fqn];
    }

    var rawNodes = raw.nodes || {};
    var tables = rawNodes.tables || raw.tables || {};
    var configs = rawNodes.configurations || raw.configurations || {};
    for (var fqn in tables) {
      var t = tables[fqn];
      ensureNode(fqn, "table", {
        columns: t.columns ? (Array.isArray(t.columns) ? t.columns.length : t.columns) : 0,
        rows: t.rows_count || t.rows || 0,
        project: t.project_alias || t.project || ""
      });
    }
    for (var cfqn in configs) {
      var c = configs[cfqn];
      ensureNode(cfqn, "config", {
        name: c.config_name || c.name || cfqn,
        component: c.component_id || c.component || "",
        component_type: c.component_type || "",
        project: c.project_alias || c.project || ""
      });
    }

    for (var i = 0; i < edges.length; i++) {
      var e = edges[i];
      var srcFqn = e.source || e.from;
      var tgtFqn = e.target || e.to;
      if (!srcFqn || !tgtFqn) continue;
      var srcNode = ensureNode(srcFqn, null, {});
      var tgtNode = ensureNode(tgtFqn, null, {});
      srcNode.conns++;
      tgtNode.conns++;
      var srcProject = srcFqn.split(":")[0] || "";
      var tgtProject = tgtFqn.split(":")[0] || "";
      links.push({
        source: srcFqn, target: tgtFqn,
        detection: e.detection || e.type || "",
        crossProject: srcProject !== tgtProject,
        columns: e.columns || [],
        column_mapping: e.column_mapping || {}
      });
    }

    var nodes = Object.values(nodeMap);
    return { nodes: nodes, links: links };
  }

  function renderGraph(graph) {
    var svg = d3.select("svg").attr("width", WIDTH).attr("height", HEIGHT);
    var tooltip = d3.select("#tooltip");
    var searchInput = d3.select("#search");
    var stats = d3.select("#stats");

    stats.text(graph.nodes.length + " nodes, " + graph.links.length + " edges");

    var defs = svg.append("defs");
    var markerTypes = ["normal", "cross", "upstream", "downstream", "dimmed"];
    var markerColors = {
      normal: "#555", cross: "#ff8a65",
      upstream: "#ef5350", downstream: "#42a5f5", dimmed: "#333"
    };
    markerTypes.forEach(function(mt) {
      defs.append("marker")
        .attr("id", "arrow-" + mt).attr("viewBox", "0 -5 10 10")
        .attr("refX", 20).attr("refY", 0)
        .attr("markerWidth", 6).attr("markerHeight", 6)
        .attr("orient", "auto")
        .append("path").attr("d", "M0,-5L10,0L0,5")
        .attr("fill", markerColors[mt]);
    });

    var g = svg.append("g");

    var zoom = d3.zoom()
      .scaleExtent([0.1, 8])
      .on("zoom", function(e) { g.attr("transform", e.transform); });
    svg.call(zoom);

    var maxConns = d3.max(graph.nodes, function(d) { return d.conns; }) || 1;
    var rScale = d3.scaleSqrt().domain([0, maxConns]).range([5, 20]);

    var sim = d3.forceSimulation(graph.nodes)
      .force("link", d3.forceLink(graph.links).id(function(d) { return d.id; }).distance(100))
      .force("charge", d3.forceManyBody().strength(-200))
      .force("center", d3.forceCenter(WIDTH / 2, HEIGHT / 2))
      .force("collision", d3.forceCollide().radius(function(d) {
        return rScale(d.conns) + 4;
      }));

    var link = g.append("g").selectAll("line")
      .data(graph.links).join("line")
      .attr("class", "link")
      .attr("stroke", function(d) { return d.crossProject ? "#ff8a65" : "#555"; })
      .attr("stroke-width", function(d) { return d.crossProject ? 1.8 : 1; })
      .attr("marker-end", function(d) {
        return "url(#arrow-" + (d.crossProject ? "cross" : "normal") + ")";
      });

    var linkLabel = g.append("g").selectAll("text")
      .data(graph.links).join("text")
      .attr("class", "link-label")
      .text(function(d) { return d.detection; });

    var node = g.append("g").selectAll(".node")
      .data(graph.nodes).join(function(enter) {
        return enter.append(function(d) {
          return document.createElementNS("http://www.w3.org/2000/svg",
            d.type === "table" ? "circle" : "rect");
        });
      })
      .attr("class", "node")
      .attr("fill", function(d) { return d.type === "table" ? "#4fc3f7" : "#81c784"; })
      .attr("stroke", function(d) { return d.type === "table" ? "#29b6f6" : "#66bb6a"; })
      .each(function(d) {
        var el = d3.select(this);
        var r = rScale(d.conns);
        if (d.type === "table") {
          el.attr("r", r);
        } else {
          el.attr("width", r * 2).attr("height", r * 2)
            .attr("rx", 3).attr("ry", 3);
        }
      })
      .call(d3.drag()
        .on("start", function(e, d) {
          if (!e.active) sim.alphaTarget(0.3).restart();
          d.fx = d.x; d.fy = d.y;
        })
        .on("drag", function(e, d) { d.fx = e.x; d.fy = e.y; })
        .on("end", function(e, d) {
          if (!e.active) sim.alphaTarget(0);
          d.fx = null; d.fy = null;
        })
      );

    var nodeLabel = g.append("g").selectAll("text")
      .data(graph.nodes).join("text")
      .attr("class", "node-label")
      .text(function(d) {
        var parts = d.id.split(":");
        var name = parts.length > 1 ? parts[1] : parts[0];
        return name.length > 30 ? name.slice(0, 28) + ".." : name;
      })
      .attr("dy", function(d) { return rScale(d.conns) + 12; });

    var adjSrc = {};
    var adjTgt = {};
    graph.links.forEach(function(l, i) {
      var sid = typeof l.source === "object" ? l.source.id : l.source;
      var tid = typeof l.target === "object" ? l.target.id : l.target;
      if (!adjSrc[sid]) adjSrc[sid] = [];
      adjSrc[sid].push(i);
      if (!adjTgt[tid]) adjTgt[tid] = [];
      adjTgt[tid].push(i);
    });

    function getUpstream(nodeId, visited) {
      visited = visited || {};
      if (visited[nodeId]) return [];
      visited[nodeId] = true;
      var indices = adjTgt[nodeId] || [];
      var result = indices.slice();
      for (var j = 0; j < indices.length; j++) {
        var l = graph.links[indices[j]];
        var sid = typeof l.source === "object" ? l.source.id : l.source;
        result = result.concat(getUpstream(sid, visited));
      }
      return result;
    }

    function getDownstream(nodeId, visited) {
      visited = visited || {};
      if (visited[nodeId]) return [];
      visited[nodeId] = true;
      var indices = adjSrc[nodeId] || [];
      var result = indices.slice();
      for (var j = 0; j < indices.length; j++) {
        var l = graph.links[indices[j]];
        var tid = typeof l.target === "object" ? l.target.id : l.target;
        result = result.concat(getDownstream(tid, visited));
      }
      return result;
    }

    var selectedNode = null;

    node.on("click", function(event, d) {
      event.stopPropagation();
      if (selectedNode === d.id) {
        selectedNode = null;
        resetHighlight();
        return;
      }
      selectedNode = d.id;
      var upIdx = {};
      getUpstream(d.id).forEach(function(idx) { upIdx[idx] = true; });
      var downIdx = {};
      getDownstream(d.id).forEach(function(idx) { downIdx[idx] = true; });

      link.attr("stroke", function(l, i) {
        if (upIdx[i]) return "#ef5350";
        if (downIdx[i]) return "#42a5f5";
        return "#333";
      }).attr("stroke-opacity", function(l, i) {
        return (upIdx[i] || downIdx[i]) ? 0.85 : 0.1;
      }).attr("marker-end", function(l, i) {
        if (upIdx[i]) return "url(#arrow-upstream)";
        if (downIdx[i]) return "url(#arrow-downstream)";
        return "url(#arrow-dimmed)";
      });

      var connectedNodes = {};
      connectedNodes[d.id] = true;
      var allIdx = Object.keys(upIdx).concat(Object.keys(downIdx));
      allIdx.forEach(function(i) {
        var l = graph.links[i];
        var sid = typeof l.source === "object" ? l.source.id : l.source;
        var tid = typeof l.target === "object" ? l.target.id : l.target;
        connectedNodes[sid] = true;
        connectedNodes[tid] = true;
      });

      node.attr("opacity", function(n) { return connectedNodes[n.id] ? 1 : 0.15; })
        .attr("stroke", function(n) {
          if (n.id === d.id) return "#ffeb3b";
          return n.type === "table" ? "#29b6f6" : "#66bb6a";
        })
        .attr("stroke-width", function(n) { return n.id === d.id ? 3 : 1.5; });
      nodeLabel.attr("opacity", function(n) { return connectedNodes[n.id] ? 1 : 0.1; });
      linkLabel.attr("opacity", function(l, i) {
        return (upIdx[i] || downIdx[i]) ? 1 : 0.05;
      });
    });

    svg.on("click", function() { selectedNode = null; resetHighlight(); });

    function resetHighlight() {
      link.attr("stroke", function(d) { return d.crossProject ? "#ff8a65" : "#555"; })
        .attr("stroke-opacity", 0.4)
        .attr("marker-end", function(d) {
          return "url(#arrow-" + (d.crossProject ? "cross" : "normal") + ")";
        });
      node.attr("opacity", 1)
        .attr("stroke", function(d) { return d.type === "table" ? "#29b6f6" : "#66bb6a"; })
        .attr("stroke-width", 1.5);
      nodeLabel.attr("opacity", 1);
      linkLabel.attr("opacity", 1);
    }

    node.on("mouseenter", function(event, d) {
      var tt = tooltip.style("display", "block");
      var body = '<div class="tt-row">Type: ' + d.type + "</div>";
      body += '<div class="tt-row">Connections: ' + d.conns + "</div>";
      if (d.meta.project) {
        body += '<div class="tt-row">Project: ' + d.meta.project + "</div>";
      }
      if (d.type === "table") {
        body += '<div class="tt-row">Columns: ' + (d.meta.columns || 0) + "</div>";
        body += '<div class="tt-row">Rows: '
          + (d.meta.rows || 0).toLocaleString() + "</div>";
      } else {
        if (d.meta.name) {
          body += '<div class="tt-row">Name: ' + d.meta.name + "</div>";
        }
        if (d.meta.component) {
          body += '<div class="tt-row">Component: ' + d.meta.component + "</div>";
        }
        if (d.meta.component_type) {
          body += '<div class="tt-row">Type: ' + d.meta.component_type + "</div>";
        }
      }
      tt.select(".tt-title").text(d.id);
      tt.select(".tt-body").html(body);
    }).on("mousemove", function(event) {
      tooltip.style("left", (event.clientX + 14) + "px")
        .style("top", (event.clientY - 10) + "px");
    }).on("mouseleave", function() { tooltip.style("display", "none"); });

    searchInput.on("input", function() {
      var q = this.value.toLowerCase().trim();
      if (!q) { resetHighlight(); return; }
      node.attr("opacity", function(d) {
        return d.id.toLowerCase().indexOf(q) >= 0 ? 1 : 0.12;
      }).attr("stroke", function(d) {
        if (d.id.toLowerCase().indexOf(q) >= 0) return "#ffeb3b";
        return d.type === "table" ? "#29b6f6" : "#66bb6a";
      }).attr("stroke-width", function(d) {
        return d.id.toLowerCase().indexOf(q) >= 0 ? 3 : 1.5;
      });
      nodeLabel.attr("opacity", function(d) {
        return d.id.toLowerCase().indexOf(q) >= 0 ? 1 : 0.08;
      });
      link.attr("stroke-opacity", 0.12);
      linkLabel.attr("opacity", 0.08);
    });

    sim.on("tick", function() {
      link.attr("x1", function(d) { return d.source.x; })
        .attr("y1", function(d) { return d.source.y; })
        .attr("x2", function(d) { return d.target.x; })
        .attr("y2", function(d) { return d.target.y; });
      linkLabel
        .attr("x", function(d) { return (d.source.x + d.target.x) / 2; })
        .attr("y", function(d) { return (d.source.y + d.target.y) / 2; });
      node.each(function(d) {
        var el = d3.select(this);
        if (d.type === "table") {
          el.attr("cx", d.x).attr("cy", d.y);
        } else {
          var r = rScale(d.conns);
          el.attr("x", d.x - r).attr("y", d.y - r);
        }
      });
      nodeLabel.attr("x", function(d) { return d.x; })
        .attr("y", function(d) { return d.y; });
    });
  }
})();
</script>
</body>
</html>"""


class _LineageHandler(http.server.BaseHTTPRequestHandler):
    """HTTP handler that serves the lineage visualization page and data."""

    html_content: str = ""
    json_content: str = ""

    def do_GET(self) -> None:
        if self.path == "/" or self.path == "/index.html":
            self._serve(self.html_content, "text/html")
        elif self.path == "/data.json":
            self._serve(self.json_content, "application/json")
        else:
            self.send_error(404)

    def _serve(self, content: str, content_type: str) -> None:
        encoded = content.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: object) -> None:
        """Silence default stderr logging."""


@lineage_app.command("serve")
def lineage_serve(
    ctx: typer.Context,
    load: Path = typer.Option(
        ...,
        "--load",
        "-l",
        help="Lineage JSON cache file (from `lineage build`).",
    ),
    port: int = typer.Option(
        8088,
        "--port",
        help="Port to serve on.",
    ),
    host: str = typer.Option(
        "127.0.0.1",
        "--host",
        help="Host to bind to.",
    ),
) -> None:
    """Start a local web server with interactive D3.js lineage visualization.

    Serves an interactive force-directed graph from a cached lineage file.
    Nodes represent tables (circles) and configurations (squares).
    Click a node to highlight upstream (red) and downstream (blue) paths.

    Example:

      kbagent lineage serve -l lineage.json
      kbagent lineage serve -l lineage.json --port 9000
    """
    formatter = get_formatter(ctx)

    if not load.exists():
        formatter.error(message=f"Cache file not found: {load}", error_code="FILE_NOT_FOUND")
        raise typer.Exit(code=1)

    try:
        raw_data = json.loads(load.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        formatter.error(message=f"Cannot read lineage file: {exc}", error_code="READ_ERROR")
        raise typer.Exit(code=1) from None

    # Attach content to the handler class
    _LineageHandler.html_content = _LINEAGE_HTML_TEMPLATE
    _LineageHandler.json_content = json.dumps(raw_data)

    server = http.server.HTTPServer((host, port), _LineageHandler)
    url = f"http://{host}:{port}"

    if formatter.json_mode:
        formatter.output({"url": url, "host": host, "port": port})
    else:
        formatter.console.print("\n[bold]Lineage visualization server[/bold]")
        formatter.console.print(f"  URL: {url}")
        formatter.console.print(f"  Data: {load.resolve()}")
        formatter.console.print("  Press Ctrl+C to stop.\n")

    # Open browser in a separate thread to avoid blocking
    threading.Thread(target=webbrowser.open, args=(url,), daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        if not formatter.json_mode:
            formatter.console.print("\nServer stopped.")
