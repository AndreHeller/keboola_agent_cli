"""Hint definitions for lineage commands."""

from .. import HintRegistry
from ..models import ClientCall, CommandHint, HintStep, ServiceCall

# ── lineage build ─────────────────────────────────────────────────

HintRegistry.register(
    CommandHint(
        cli_command="lineage.build",
        description="Build column-level lineage graph from sync'd data",
        steps=[
            HintStep(
                comment="Build lineage graph from sync'd projects",
                client=ClientCall(
                    method="build_lineage",
                    args={"root": "{directory}"},
                    result_var="lineage_data",
                    result_hint="dict",
                    client_type="storage",
                ),
                service=ServiceCall(
                    service_class="DeepLineageService",
                    service_module="deep_lineage_service",
                    method="build_lineage",
                    args={
                        "root": "{directory}",
                        "generate_ai_tasks": "{ai}",
                    },
                ),
            ),
        ],
        notes=[
            "This command reads from disk (sync'd data), not from the API. "
            "Run 'kbagent sync pull --all-projects' first.",
            "No --hint client equivalent — use --hint service to get Python code.",
            "AI is 2-step: build with generate_ai_tasks=True writes .lineage_ai_tasks.json. "
            "AI agent processes tasks and writes .lineage_ai_results.json. "
            "Re-run build_lineage() to apply.",
        ],
    )
)

# ── lineage show ──────────────────────────────────────────────────

HintRegistry.register(
    CommandHint(
        cli_command="lineage.show",
        description="Query upstream/downstream from cached lineage graph",
        steps=[
            HintStep(
                comment="Load lineage graph from cache",
                client=ClientCall(
                    method="load_from_cache",
                    args={"cache_path": "{load}"},
                    result_var="graph",
                    result_hint="LineageGraph",
                    client_type="storage",
                ),
                service=ServiceCall(
                    service_class="DeepLineageService",
                    service_module="deep_lineage_service",
                    method="load_from_cache",
                    args={"cache_path": "{load}"},
                ),
            ),
            HintStep(
                comment="Query upstream dependencies",
                client=ClientCall(
                    method="query_upstream",
                    args={
                        "identifier": "{upstream}",
                        "project": "{project}",
                        "depth": "{depth}",
                    },
                    result_var="upstream_result",
                    result_hint="dict",
                    client_type="storage",
                ),
                service=ServiceCall(
                    service_class="DeepLineageService",
                    service_module="deep_lineage_service",
                    method="query_upstream",
                    args={
                        "graph": "graph",
                        "identifier": "{upstream}",
                        "project": "{project}",
                        "depth": "{depth}",
                    },
                ),
            ),
            HintStep(
                comment="Query downstream dependents",
                client=ClientCall(
                    method="query_downstream",
                    args={
                        "identifier": "{downstream}",
                        "project": "{project}",
                        "depth": "{depth}",
                    },
                    result_var="downstream_result",
                    result_hint="dict",
                    client_type="storage",
                ),
                service=ServiceCall(
                    service_class="DeepLineageService",
                    service_module="deep_lineage_service",
                    method="query_downstream",
                    args={
                        "graph": "graph",
                        "identifier": "{downstream}",
                        "project": "{project}",
                        "depth": "{depth}",
                    },
                ),
            ),
        ],
        notes=[
            "Load the cache file first, then pass the graph to query methods.",
            "No --hint client equivalent — use --hint service to get Python code.",
        ],
    )
)
