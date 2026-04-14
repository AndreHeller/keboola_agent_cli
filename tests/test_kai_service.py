"""Tests for KaiService — Keboola AI Assistant business logic.

Tests the sync wrapper methods (ping, ask, chat_message, get_history)
with mocked KaiClient and feature-flag detection.
"""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from helpers import setup_single_project
from keboola_agent_cli.errors import ConfigError, KeboolaApiError
from keboola_agent_cli.models import TokenVerifyResponse
from keboola_agent_cli.services.kai_service import KaiService


def _make_kai_service(tmp_config_dir: Path, features: list[str] | None = None):
    """Create a KaiService with a mock client that returns given features."""
    store = setup_single_project(tmp_config_dir)
    mock_client = MagicMock()
    mock_client.verify_token.return_value = TokenVerifyResponse(
        token_id="t-123",
        token_description="test token",
        project_id=258,
        project_name="Production",
        owner_name="Production",
        features=features or [],
    )
    mock_client.close.return_value = None

    service = KaiService(
        config_store=store,
        client_factory=lambda url, token: mock_client,
    )
    return service, mock_client


class TestKaiServicePing:
    """Tests for KaiService.ping()."""

    def test_ping_success(self, tmp_config_dir: Path) -> None:
        """ping returns server health info when Kai is enabled."""
        service, _ = _make_kai_service(tmp_config_dir, features=["agent-chat"])

        mock_ping_resp = MagicMock()
        mock_ping_resp.timestamp = datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC)

        mock_info_resp = MagicMock()
        mock_info_resp.app_name = "kai-api"
        mock_info_resp.app_version = "1.2.3"
        mock_info_resp.server_version = "2.0.0"
        mock_info_resp.connected_mcp = {"status": "connected"}

        mock_kai_client = AsyncMock()
        mock_kai_client.ping.return_value = mock_ping_resp
        mock_kai_client.info.return_value = mock_info_resp
        mock_kai_client.__aenter__ = AsyncMock(return_value=mock_kai_client)
        mock_kai_client.__aexit__ = AsyncMock(return_value=False)

        with patch.object(service, "_create_kai_client", return_value=mock_kai_client):
            result = service.ping("prod")

        assert result["project_alias"] == "prod"
        assert result["timestamp"] == "2025-01-15T10:30:00+00:00"
        assert result["app_name"] == "kai-api"
        assert result["app_version"] == "1.2.3"
        assert result["server_version"] == "2.0.0"
        assert result["mcp_status"] == "connected"

    def test_ping_kai_not_enabled(self, tmp_config_dir: Path) -> None:
        """ping raises KeboolaApiError when agent-chat feature flag is missing."""
        service, _ = _make_kai_service(tmp_config_dir, features=[])

        with pytest.raises(KeboolaApiError) as exc_info:
            service.ping("prod")

        assert exc_info.value.error_code == "KAI_NOT_ENABLED"
        assert "Kai is not enabled" in exc_info.value.message

    def test_ping_mcp_status_unknown(self, tmp_config_dir: Path) -> None:
        """ping returns 'unknown' when connected_mcp is not a dict."""
        service, _ = _make_kai_service(tmp_config_dir, features=["agent-chat"])

        mock_ping_resp = MagicMock()
        mock_ping_resp.timestamp = datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC)

        mock_info_resp = MagicMock()
        mock_info_resp.app_name = "kai-api"
        mock_info_resp.app_version = "1.2.3"
        mock_info_resp.server_version = "2.0.0"
        mock_info_resp.connected_mcp = "not-a-dict"

        mock_kai_client = AsyncMock()
        mock_kai_client.ping.return_value = mock_ping_resp
        mock_kai_client.info.return_value = mock_info_resp
        mock_kai_client.__aenter__ = AsyncMock(return_value=mock_kai_client)
        mock_kai_client.__aexit__ = AsyncMock(return_value=False)

        with patch.object(service, "_create_kai_client", return_value=mock_kai_client):
            result = service.ping("prod")

        assert result["mcp_status"] == "unknown"


class TestKaiServiceAsk:
    """Tests for KaiService.ask()."""

    def test_ask_success(self, tmp_config_dir: Path) -> None:
        """ask returns chat_id and response text."""
        service, _ = _make_kai_service(tmp_config_dir, features=["agent-chat"])

        mock_kai_client = AsyncMock()
        mock_kai_client.chat.return_value = ("chat-abc-123", "The answer is 42.")
        mock_kai_client.__aenter__ = AsyncMock(return_value=mock_kai_client)
        mock_kai_client.__aexit__ = AsyncMock(return_value=False)

        with patch.object(service, "_create_kai_client", return_value=mock_kai_client):
            result = service.ask("prod", "What is the answer?")

        assert result["project_alias"] == "prod"
        assert result["chat_id"] == "chat-abc-123"
        assert result["response"] == "The answer is 42."

    def test_ask_api_error(self, tmp_config_dir: Path) -> None:
        """ask wraps KaiError into KeboolaApiError."""
        from kai_client import KaiError

        service, _ = _make_kai_service(tmp_config_dir, features=["agent-chat"])

        mock_kai_client = AsyncMock()
        mock_kai_client.chat.side_effect = KaiError(message="Service unavailable")
        mock_kai_client.__aenter__ = AsyncMock(return_value=mock_kai_client)
        mock_kai_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch.object(service, "_create_kai_client", return_value=mock_kai_client),
            pytest.raises(KeboolaApiError) as exc_info,
        ):
            service.ask("prod", "test question")

        assert exc_info.value.error_code == "KAI_ERROR"
        assert "Kai ask failed" in exc_info.value.message
        assert "Service unavailable" in exc_info.value.message

    def test_ask_kai_not_enabled(self, tmp_config_dir: Path) -> None:
        """ask raises KAI_NOT_ENABLED when feature flag is missing."""
        service, _ = _make_kai_service(tmp_config_dir, features=[])

        with pytest.raises(KeboolaApiError) as exc_info:
            service.ask("prod", "some question")

        assert exc_info.value.error_code == "KAI_NOT_ENABLED"


class TestKaiServiceChat:
    """Tests for KaiService.chat_message()."""

    def test_chat_new_session(self, tmp_config_dir: Path) -> None:
        """chat_message without chat_id creates a new session."""
        service, _ = _make_kai_service(tmp_config_dir, features=["agent-chat"])

        # Create a mock event with type="text" and text attribute
        mock_event = MagicMock()
        mock_event.type = "text"
        mock_event.text = "Hello from Kai!"

        mock_kai_client = AsyncMock()
        # new_chat_id() is called without await, so use MagicMock for it
        mock_kai_client.new_chat_id = MagicMock(return_value="new-chat-id-456")

        # send_message returns an async iterator
        async def mock_send_message(cid, msg):
            yield mock_event

        mock_kai_client.send_message = mock_send_message
        mock_kai_client.__aenter__ = AsyncMock(return_value=mock_kai_client)
        mock_kai_client.__aexit__ = AsyncMock(return_value=False)

        with patch.object(service, "_create_kai_client", return_value=mock_kai_client):
            result = service.chat_message("prod", "Hello!")

        assert result["project_alias"] == "prod"
        assert result["chat_id"] == "new-chat-id-456"
        assert result["response"] == "Hello from Kai!"

    def test_chat_continue(self, tmp_config_dir: Path) -> None:
        """chat_message with chat_id continues an existing session."""
        service, _ = _make_kai_service(tmp_config_dir, features=["agent-chat"])

        mock_event1 = MagicMock()
        mock_event1.type = "text"
        mock_event1.text = "Part one. "

        mock_event2 = MagicMock()
        mock_event2.type = "text"
        mock_event2.text = "Part two."

        # Non-text event should be skipped
        mock_event_other = MagicMock()
        mock_event_other.type = "tool_call"

        mock_kai_client = AsyncMock()

        async def mock_send_message(cid, msg):
            yield mock_event1
            yield mock_event_other
            yield mock_event2

        mock_kai_client.send_message = mock_send_message
        mock_kai_client.__aenter__ = AsyncMock(return_value=mock_kai_client)
        mock_kai_client.__aexit__ = AsyncMock(return_value=False)

        with patch.object(service, "_create_kai_client", return_value=mock_kai_client):
            result = service.chat_message("prod", "Continue please", chat_id="existing-chat-789")

        assert result["chat_id"] == "existing-chat-789"
        assert result["response"] == "Part one. Part two."

    def test_chat_kai_error(self, tmp_config_dir: Path) -> None:
        """chat_message wraps KaiError into KeboolaApiError."""
        from kai_client import KaiError

        service, _ = _make_kai_service(tmp_config_dir, features=["agent-chat"])

        mock_kai_client = AsyncMock()
        mock_kai_client.new_chat_id.return_value = "chat-err"

        async def mock_send_message(cid, msg):
            raise KaiError(message="Chat session expired")
            yield  # needed to make this an async generator

        mock_kai_client.send_message = mock_send_message
        mock_kai_client.__aenter__ = AsyncMock(return_value=mock_kai_client)
        mock_kai_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch.object(service, "_create_kai_client", return_value=mock_kai_client),
            pytest.raises(KeboolaApiError) as exc_info,
        ):
            service.chat_message("prod", "test")

        assert exc_info.value.error_code == "KAI_ERROR"
        assert "Kai chat failed" in exc_info.value.message


class TestKaiServiceHistory:
    """Tests for KaiService.get_history()."""

    def test_history_success(self, tmp_config_dir: Path) -> None:
        """get_history returns a list of chat summaries."""
        service, _ = _make_kai_service(tmp_config_dir, features=["agent-chat"])

        chat1 = MagicMock()
        chat1.id = "chat-aaa"
        chat1.title = "First chat"
        chat1.created_at = datetime(2025, 1, 10, 8, 0, 0, tzinfo=UTC)
        chat1.visibility = "private"

        chat2 = MagicMock()
        chat2.id = "chat-bbb"
        chat2.title = None  # untitled
        chat2.created_at = None
        chat2.visibility = "public"

        mock_history = MagicMock()
        mock_history.chats = [chat1, chat2]
        mock_history.has_more = True

        mock_kai_client = AsyncMock()
        mock_kai_client.get_history.return_value = mock_history
        mock_kai_client.__aenter__ = AsyncMock(return_value=mock_kai_client)
        mock_kai_client.__aexit__ = AsyncMock(return_value=False)

        with patch.object(service, "_create_kai_client", return_value=mock_kai_client):
            result = service.get_history("prod", limit=5)

        assert result["project_alias"] == "prod"
        assert result["has_more"] is True
        assert len(result["chats"]) == 2

        assert result["chats"][0]["id"] == "chat-aaa"
        assert result["chats"][0]["title"] == "First chat"
        assert result["chats"][0]["created_at"] == "2025-01-10T08:00:00+00:00"
        assert result["chats"][0]["visibility"] == "private"

        assert result["chats"][1]["id"] == "chat-bbb"
        assert result["chats"][1]["title"] == "(untitled)"
        assert result["chats"][1]["created_at"] is None
        assert result["chats"][1]["visibility"] == "public"

    def test_history_empty(self, tmp_config_dir: Path) -> None:
        """get_history returns empty list when no chats exist."""
        service, _ = _make_kai_service(tmp_config_dir, features=["agent-chat"])

        mock_history = MagicMock()
        mock_history.chats = []
        mock_history.has_more = False

        mock_kai_client = AsyncMock()
        mock_kai_client.get_history.return_value = mock_history
        mock_kai_client.__aenter__ = AsyncMock(return_value=mock_kai_client)
        mock_kai_client.__aexit__ = AsyncMock(return_value=False)

        with patch.object(service, "_create_kai_client", return_value=mock_kai_client):
            result = service.get_history("prod")

        assert result["chats"] == []
        assert result["has_more"] is False

    def test_history_kai_not_enabled(self, tmp_config_dir: Path) -> None:
        """get_history raises KAI_NOT_ENABLED when feature flag is missing."""
        service, _ = _make_kai_service(tmp_config_dir, features=[])

        with pytest.raises(KeboolaApiError) as exc_info:
            service.get_history("prod")

        assert exc_info.value.error_code == "KAI_NOT_ENABLED"


class TestKaiServiceResolveAlias:
    """Tests for KaiService.resolve_alias()."""

    def test_resolve_explicit_alias(self, tmp_config_dir: Path) -> None:
        """resolve_alias with explicit alias validates and returns it."""
        service, _ = _make_kai_service(tmp_config_dir)
        assert service.resolve_alias("prod") == "prod"

    def test_resolve_default_alias(self, tmp_config_dir: Path) -> None:
        """resolve_alias with None returns the first (default) project."""
        service, _ = _make_kai_service(tmp_config_dir)
        assert service.resolve_alias(None) == "prod"

    def test_resolve_unknown_alias(self, tmp_config_dir: Path) -> None:
        """resolve_alias raises ConfigError for unknown alias."""
        service, _ = _make_kai_service(tmp_config_dir)
        with pytest.raises(ConfigError):
            service.resolve_alias("nonexistent")
