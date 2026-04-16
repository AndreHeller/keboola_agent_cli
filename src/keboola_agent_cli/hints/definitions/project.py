"""Hint definitions for project-level commands (project description)."""

from .. import HintRegistry
from ..models import ClientCall, CommandHint, HintStep, ServiceCall

# ── project description-get ───────────────────────────────────────

HintRegistry.register(
    CommandHint(
        cli_command="project.description-get",
        description="Read the Keboola dashboard project description",
        steps=[
            HintStep(
                comment="Read KBC.projectDescription on the default branch",
                client=ClientCall(
                    method="get_branch_metadata_value",
                    args={
                        "key": '"KBC.projectDescription"',
                        "branch_id": '"default"',
                    },
                    result_var="description",
                    result_hint="str | None",
                ),
                service=ServiceCall(
                    service_class="BranchService",
                    service_module="branch_service",
                    method="get_project_description",
                    args={"alias": "{project}"},
                ),
            ),
        ],
        notes=[
            "The dashboard reads project description from branch metadata, "
            "not from the Manage API or the branch description field.",
        ],
    )
)

# ── project description-set ───────────────────────────────────────

HintRegistry.register(
    CommandHint(
        cli_command="project.description-set",
        description="Set the Keboola dashboard project description (markdown)",
        steps=[
            HintStep(
                comment="Write KBC.projectDescription on the default branch",
                client=ClientCall(
                    method="set_branch_metadata",
                    args={
                        "entries": '[("KBC.projectDescription", {description})]',
                        "branch_id": '"default"',
                    },
                    result_var="result",
                    result_hint="list[dict]",
                ),
                service=ServiceCall(
                    service_class="BranchService",
                    service_module="branch_service",
                    method="set_project_description",
                    args={
                        "alias": "{project}",
                        "description": "{description}",
                    },
                ),
            ),
        ],
        notes=[
            "Writes to the default branch metadata - always the main branch, "
            "regardless of any active dev branch.",
        ],
    )
)
