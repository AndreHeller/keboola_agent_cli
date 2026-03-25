"""Tests for VersionService - version detection and update checks."""

from unittest.mock import MagicMock, patch

from keboola_agent_cli.services.version_service import (
    VersionService,
    _fetch_mcp_latest_version,
    _is_up_to_date,
    _is_uvx_available,
)


class TestIsUvxAvailable:
    """Tests for _is_uvx_available()."""

    @patch("keboola_agent_cli.services.version_service.shutil.which")
    def test_uvx_found(self, mock_which: MagicMock) -> None:
        mock_which.return_value = "/usr/local/bin/uvx"
        assert _is_uvx_available() is True

    @patch("keboola_agent_cli.services.version_service.shutil.which")
    def test_uvx_not_found(self, mock_which: MagicMock) -> None:
        mock_which.return_value = None
        assert _is_uvx_available() is False


class TestFetchMcpLatestVersion:
    """Tests for _fetch_mcp_latest_version()."""

    @patch("keboola_agent_cli.services.version_service.httpx.get")
    def test_success(self, mock_get: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = {"info": {"version": "1.46.0"}}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        assert _fetch_mcp_latest_version() == "1.46.0"

    @patch("keboola_agent_cli.services.version_service.httpx.get")
    def test_http_error(self, mock_get: MagicMock) -> None:
        import httpx

        mock_get.side_effect = httpx.HTTPError("connection failed")
        assert _fetch_mcp_latest_version() is None


class TestIsUpToDate:
    """Tests for _is_up_to_date()."""

    def test_same_version(self) -> None:
        assert _is_up_to_date("2.44.0", "2.44.0") is True

    def test_newer_available(self) -> None:
        assert _is_up_to_date("2.44.0", "2.44.2") is False

    def test_local_newer(self) -> None:
        assert _is_up_to_date("2.45.0", "2.44.2") is True

    def test_local_none(self) -> None:
        assert _is_up_to_date(None, "2.44.2") is None

    def test_latest_none(self) -> None:
        assert _is_up_to_date("2.44.0", None) is None

    def test_both_none(self) -> None:
        assert _is_up_to_date(None, None) is None

    def test_invalid_version(self) -> None:
        assert _is_up_to_date("not-a-version", "2.44.0") is None


class TestVersionService:
    """Tests for VersionService.get_versions()."""

    @patch("keboola_agent_cli.services.version_service._fetch_mcp_latest_version")
    @patch("keboola_agent_cli.services.version_service._is_uvx_available")
    def test_mcp_auto_updates(
        self,
        mock_uvx: MagicMock,
        mock_mcp_latest: MagicMock,
    ) -> None:
        mock_uvx.return_value = True
        mock_mcp_latest.return_value = "1.46.0"

        svc = VersionService()
        result = svc.get_versions()

        assert result["kbagent"]["version"] is not None
        deps = result["dependencies"]
        assert len(deps) == 1

        mcp_dep = deps[0]
        assert mcp_dep["name"] == "keboola-mcp-server"
        assert mcp_dep["auto_updates"] is True
        assert mcp_dep["uvx_available"] is True
        assert mcp_dep["latest_version"] == "1.46.0"

    @patch("keboola_agent_cli.services.version_service._fetch_mcp_latest_version")
    @patch("keboola_agent_cli.services.version_service._is_uvx_available")
    def test_uvx_not_available(
        self,
        mock_uvx: MagicMock,
        mock_mcp_latest: MagicMock,
    ) -> None:
        mock_uvx.return_value = False
        mock_mcp_latest.return_value = "1.46.0"

        svc = VersionService()
        result = svc.get_versions()

        mcp_dep = result["dependencies"][0]
        assert mcp_dep["uvx_available"] is False

    @patch("keboola_agent_cli.services.version_service._fetch_mcp_latest_version")
    @patch("keboola_agent_cli.services.version_service._is_uvx_available")
    def test_remote_check_fails(
        self,
        mock_uvx: MagicMock,
        mock_mcp_latest: MagicMock,
    ) -> None:
        mock_uvx.return_value = True
        mock_mcp_latest.return_value = None

        svc = VersionService()
        result = svc.get_versions()

        mcp_dep = result["dependencies"][0]
        assert mcp_dep["latest_version"] is None
