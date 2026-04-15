"""Hint definitions for lineage commands."""

from .. import HintRegistry
from ..models import ClientCall, CommandHint, HintStep, ServiceCall

# ── lineage show ───────────────────────────────────────────────────

HintRegistry.register(
    CommandHint(
        cli_command="lineage.show",
        description="Show cross-project data lineage via bucket sharing",
        steps=[
            HintStep(
                comment="List buckets with linked-bucket metadata",
                client=ClientCall(
                    method="list_buckets",
                    args={"include": '"linkedBuckets"'},
                    result_var="buckets",
                    result_hint="list[dict]",
                ),
                service=ServiceCall(
                    service_class="LineageService",
                    service_module="lineage_service",
                    method="get_lineage",
                    args={"aliases": "{project}"},
                ),
            ),
        ],
        notes=[
            "Client layer returns raw buckets — analyze sharing metadata yourself.",
            "Service layer queries all projects in parallel and builds "
            "data flow edges with deduplication.",
        ],
    )
)


# ── lineage deep ──────────────────────────────────────────────────

HintRegistry.register(
    CommandHint(
        cli_command="lineage.deep",
        description="Column-level lineage from sync'd data on disk",
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
                        "include_ai": "{ai}",
                        "ai_model": "{ai_model}",
                        "ai_workers": "{ai_workers}",
                    },
                ),
            ),
            HintStep(
                comment="Query upstream dependencies (optional)",
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
                comment="Query downstream dependents (optional)",
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
            "This command reads from disk (sync'd data), not from the API. "
            "Run 'kbagent sync pull --all-projects' first.",
            "No --hint client equivalent — use --hint service to get Python code.",
            "The graph object from build_lineage() can be converted with "
            "_graph_from_dict(lineage_data) for query methods.",
            "For cached usage: service.build_and_cache(root, Path('lineage.json')) "
            "then service.load_from_cache(Path('lineage.json')).",
        ],
    )
)
