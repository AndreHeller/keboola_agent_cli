"""Hint definitions for branch commands (list, create, delete)."""

from .. import HintRegistry
from ..models import ClientCall, CommandHint, HintStep, ServiceCall

# ── branch list ────────────────────────────────────────────────────

HintRegistry.register(
    CommandHint(
        cli_command="branch.list",
        description="List development branches",
        steps=[
            HintStep(
                comment="List dev branches",
                client=ClientCall(
                    method="list_dev_branches",
                    args={},
                    result_var="branches",
                    result_hint="list[dict]",
                ),
                service=ServiceCall(
                    service_class="BranchService",
                    service_module="branch_service",
                    method="list_branches",
                    args={"aliases": "{project}"},
                ),
            ),
        ],
    )
)

# ── branch create ──────────────────────────────────────────────────

HintRegistry.register(
    CommandHint(
        cli_command="branch.create",
        description="Create a new development branch",
        steps=[
            HintStep(
                comment="Create dev branch",
                client=ClientCall(
                    method="create_dev_branch",
                    args={"name": "{name}", "description": "{description}"},
                    result_var="branch",
                    result_hint="dict",
                ),
                service=ServiceCall(
                    service_class="BranchService",
                    service_module="branch_service",
                    method="create_branch",
                    args={
                        "alias": "{project}",
                        "name": "{name}",
                        "description": "{description}",
                    },
                ),
            ),
        ],
        notes=["Service layer also auto-activates the branch in CLI config."],
    )
)

# ── branch delete ──────────────────────────────────────────────────

HintRegistry.register(
    CommandHint(
        cli_command="branch.delete",
        description="Delete a development branch",
        steps=[
            HintStep(
                comment="Delete dev branch",
                client=ClientCall(
                    method="delete_dev_branch",
                    args={"branch_id": "{branch}"},
                    result_var="result",
                ),
                service=ServiceCall(
                    service_class="BranchService",
                    service_module="branch_service",
                    method="delete_branch",
                    args={"alias": "{project}", "branch_id": "{branch}"},
                ),
            ),
        ],
        notes=["Service layer auto-resets active branch if the deleted branch was active."],
    )
)

# ── branch metadata-list ──────────────────────────────────────────

HintRegistry.register(
    CommandHint(
        cli_command="branch.metadata-list",
        description="List metadata entries on a branch",
        steps=[
            HintStep(
                comment="List branch metadata",
                client=ClientCall(
                    method="list_branch_metadata",
                    args={"branch_id": "{branch}"},
                    result_var="metadata",
                    result_hint="list[dict]",
                ),
                service=ServiceCall(
                    service_class="BranchService",
                    service_module="branch_service",
                    method="list_branch_metadata",
                    args={"alias": "{project}", "branch_id": "{branch}"},
                ),
            ),
        ],
    )
)

# ── branch metadata-get ───────────────────────────────────────────

HintRegistry.register(
    CommandHint(
        cli_command="branch.metadata-get",
        description="Read a single metadata value by key",
        steps=[
            HintStep(
                comment="Get a branch metadata value by key",
                client=ClientCall(
                    method="get_branch_metadata_value",
                    args={"key": "{key}", "branch_id": "{branch}"},
                    result_var="value",
                    result_hint="str | None",
                ),
                service=ServiceCall(
                    service_class="BranchService",
                    service_module="branch_service",
                    method="get_branch_metadata",
                    args={
                        "alias": "{project}",
                        "key": "{key}",
                        "branch_id": "{branch}",
                    },
                ),
            ),
        ],
        notes=["Client returns None when the key is absent; service raises KeboolaApiError."],
    )
)

# ── branch metadata-set ───────────────────────────────────────────

HintRegistry.register(
    CommandHint(
        cli_command="branch.metadata-set",
        description="Set a metadata key/value on a branch",
        steps=[
            HintStep(
                comment="Set branch metadata (bulk-capable; one entry here)",
                client=ClientCall(
                    method="set_branch_metadata",
                    args={
                        "entries": "[({key}, {value})]",
                        "branch_id": "{branch}",
                    },
                    result_var="result",
                    result_hint="list[dict]",
                ),
                service=ServiceCall(
                    service_class="BranchService",
                    service_module="branch_service",
                    method="set_branch_metadata",
                    args={
                        "alias": "{project}",
                        "key": "{key}",
                        "value": "{value}",
                        "branch_id": "{branch}",
                    },
                ),
            ),
        ],
        notes=[
            "Keboola's endpoint is bulk: pass multiple (key, value) tuples to set many at once.",
        ],
    )
)

# ── branch metadata-delete ────────────────────────────────────────

HintRegistry.register(
    CommandHint(
        cli_command="branch.metadata-delete",
        description="Delete a branch metadata entry by ID",
        steps=[
            HintStep(
                comment="Delete a single metadata entry",
                client=ClientCall(
                    method="delete_branch_metadata",
                    args={
                        "metadata_id": "{metadata_id}",
                        "branch_id": "{branch}",
                    },
                    result_var="result",
                ),
                service=ServiceCall(
                    service_class="BranchService",
                    service_module="branch_service",
                    method="delete_branch_metadata",
                    args={
                        "alias": "{project}",
                        "metadata_id": "{metadata_id}",
                        "branch_id": "{branch}",
                    },
                ),
            ),
        ],
    )
)
