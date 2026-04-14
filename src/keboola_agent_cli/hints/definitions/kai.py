"""Hint definitions for Kai (Keboola AI Assistant) commands (ping, ask, chat, history)."""

from .. import HintRegistry
from ..models import ClientCall, CommandHint, HintStep, ServiceCall

# ── kai ping ──────────────────────────────────────────────────────

HintRegistry.register(
    CommandHint(
        cli_command="kai.ping",
        description="Check Kai server health and MCP connection status",
        steps=[
            HintStep(
                comment="Verify token and check Kai feature flag",
                client=ClientCall(
                    method="verify_token",
                    args={},
                    result_var="token_info",
                    result_hint="TokenInfo",
                ),
                service=ServiceCall(
                    service_class="KaiService",
                    service_module="kai_service",
                    method="ping",
                    args={"alias": "{project}"},
                ),
            ),
        ],
        notes=[
            "Kai commands use KaiClient from the 'kai_client' package, not KeboolaClient.",
            "KaiClient.from_storage_api() auto-discovers the Kai API URL from the stack URL.",
            "The service checks the 'agent-chat' feature flag before calling Kai.",
            "Client hint shows verify_token (feature detection); actual ping uses KaiClient.",
        ],
    )
)

# ── kai ask ───────────────────────────────────────────────────────

HintRegistry.register(
    CommandHint(
        cli_command="kai.ask",
        description="Ask Kai a one-shot question about your project",
        steps=[
            HintStep(
                comment="Send a one-shot question to Kai and collect the full response",
                client=ClientCall(
                    method="verify_token",
                    args={},
                    result_var="token_info",
                    result_hint="TokenInfo",
                ),
                service=ServiceCall(
                    service_class="KaiService",
                    service_module="kai_service",
                    method="ask",
                    args={
                        "alias": "{project}",
                        "message": "{message}",
                    },
                ),
            ),
        ],
        notes=[
            "Kai commands use KaiClient from the 'kai_client' package, not KeboolaClient.",
            "KaiClient.chat(message) sends a question and returns (chat_id, response_text).",
            "Service returns {'project_alias', 'chat_id', 'response'}.",
        ],
    )
)

# ── kai chat ──────────────────────────────────────────────────────

HintRegistry.register(
    CommandHint(
        cli_command="kai.chat",
        description="Send a message in a Kai chat session (new or continued)",
        steps=[
            HintStep(
                comment="Send a chat message to Kai, optionally continuing an existing session",
                client=ClientCall(
                    method="verify_token",
                    args={},
                    result_var="token_info",
                    result_hint="TokenInfo",
                ),
                service=ServiceCall(
                    service_class="KaiService",
                    service_module="kai_service",
                    method="chat_message",
                    args={
                        "alias": "{project}",
                        "message": "{message}",
                        "chat_id": "{chat_id}",
                    },
                ),
            ),
        ],
        notes=[
            "Kai commands use KaiClient from the 'kai_client' package, not KeboolaClient.",
            "Without --chat-id, starts a new chat session.",
            "With --chat-id, continues an existing conversation.",
            "KaiClient.send_message() returns an async stream of events; service collects text events.",
            "Service returns {'project_alias', 'chat_id', 'response'}.",
        ],
    )
)

# ── kai history ───────────────────────────────────────────────────

HintRegistry.register(
    CommandHint(
        cli_command="kai.history",
        description="List recent Kai chat sessions",
        steps=[
            HintStep(
                comment="Get chat history for the current user",
                client=ClientCall(
                    method="verify_token",
                    args={},
                    result_var="token_info",
                    result_hint="TokenInfo",
                ),
                service=ServiceCall(
                    service_class="KaiService",
                    service_module="kai_service",
                    method="get_history",
                    args={
                        "alias": "{project}",
                        "limit": "{limit}",
                    },
                ),
            ),
        ],
        notes=[
            "Kai commands use KaiClient from the 'kai_client' package, not KeboolaClient.",
            "Service returns {'project_alias', 'chats': [...], 'has_more': bool}.",
            "Each chat has: id, title, created_at, visibility.",
        ],
    )
)
