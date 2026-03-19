"""Tests for KbcService - kbc CLI integration for LLM export."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from helpers import setup_single_project, setup_two_projects
from keboola_agent_cli.errors import ConfigError
from keboola_agent_cli.services.kbc_service import (
    KbcService,
    _extract_host_from_url,
    detect_kbc_command,
)


class TestDetectKbcCommand:
    """Tests for detect_kbc_command()."""

    @patch("keboola_agent_cli.services.kbc_service.shutil.which")
    def test_kbc_found(self, mock_which: MagicMock) -> None:
        mock_which.return_value = "/usr/local/bin/kbc"
        assert detect_kbc_command() == "/usr/local/bin/kbc"
        mock_which.assert_called_once_with("kbc")

    @patch("keboola_agent_cli.services.kbc_service.shutil.which")
    def test_kbc_not_found(self, mock_which: MagicMock) -> None:
        mock_which.return_value = None
        assert detect_kbc_command() is None


class TestExtractHostFromUrl:
    """Tests for _extract_host_from_url()."""

    def test_standard_url(self) -> None:
        assert _extract_host_from_url("https://connection.keboola.com") == "connection.keboola.com"

    def test_azure_url(self) -> None:
        result = _extract_host_from_url("https://connection.north-europe.azure.keboola.com")
        assert result == "connection.north-europe.azure.keboola.com"

    def test_url_with_path(self) -> None:
        assert (
            _extract_host_from_url("https://connection.keboola.com/v2/") == "connection.keboola.com"
        )

    def test_empty_url_raises(self) -> None:
        with pytest.raises(ValueError, match="Cannot extract hostname"):
            _extract_host_from_url("")


class TestKbcServiceCheckAvailable:
    """Tests for KbcService.check_kbc_available()."""

    @patch("keboola_agent_cli.services.kbc_service.detect_kbc_command")
    def test_kbc_not_installed(self, mock_detect: MagicMock, tmp_config_dir: Path) -> None:
        mock_detect.return_value = None
        store = setup_single_project(tmp_config_dir)
        svc = KbcService(config_store=store)

        result = svc.check_kbc_available()
        assert result["status"] == "warn"
        assert "not found" in result["message"]

    @patch("keboola_agent_cli.services.kbc_service.subprocess.run")
    @patch("keboola_agent_cli.services.kbc_service.detect_kbc_command")
    def test_kbc_installed_with_version(
        self, mock_detect: MagicMock, mock_run: MagicMock, tmp_config_dir: Path
    ) -> None:
        mock_detect.return_value = "/usr/local/bin/kbc"
        mock_run.return_value = MagicMock(stdout="Version:    2.44.0\nGit commit: abc123\n")

        store = setup_single_project(tmp_config_dir)
        svc = KbcService(config_store=store)

        result = svc.check_kbc_available()
        assert result["status"] == "pass"
        assert "v2.44.0" in result["message"]


class TestKbcServiceGetVersion:
    """Tests for KbcService.get_kbc_version()."""

    @patch("keboola_agent_cli.services.kbc_service.detect_kbc_command")
    def test_not_installed(self, mock_detect: MagicMock) -> None:
        mock_detect.return_value = None
        assert KbcService.get_kbc_version() is None

    @patch("keboola_agent_cli.services.kbc_service.subprocess.run")
    @patch("keboola_agent_cli.services.kbc_service.detect_kbc_command")
    def test_version_parsed(self, mock_detect: MagicMock, mock_run: MagicMock) -> None:
        mock_detect.return_value = "/usr/local/bin/kbc"
        mock_run.return_value = MagicMock(stdout="Version:    2.44.0\nGit commit: abc123\n")
        assert KbcService.get_kbc_version() == "2.44.0"

    @patch("keboola_agent_cli.services.kbc_service.subprocess.run")
    @patch("keboola_agent_cli.services.kbc_service.detect_kbc_command")
    def test_version_unparseable(self, mock_detect: MagicMock, mock_run: MagicMock) -> None:
        mock_detect.return_value = "/usr/local/bin/kbc"
        mock_run.return_value = MagicMock(stdout="unknown output")
        assert KbcService.get_kbc_version() is None

    @patch("keboola_agent_cli.services.kbc_service.subprocess.run")
    @patch("keboola_agent_cli.services.kbc_service.detect_kbc_command")
    def test_timeout(self, mock_detect: MagicMock, mock_run: MagicMock) -> None:
        import subprocess

        mock_detect.return_value = "/usr/local/bin/kbc"
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="kbc", timeout=5)
        assert KbcService.get_kbc_version() is None


class TestKbcServiceRunLlmExport:
    """Tests for KbcService.run_llm_export()."""

    @patch("keboola_agent_cli.services.kbc_service.detect_kbc_command")
    def test_kbc_not_found_raises(self, mock_detect: MagicMock, tmp_config_dir: Path) -> None:
        mock_detect.return_value = None
        store = setup_single_project(tmp_config_dir)
        svc = KbcService(config_store=store)

        with pytest.raises(ConfigError, match="kbc binary not found"):
            svc.run_llm_export(alias="prod")

    @patch("keboola_agent_cli.services.kbc_service.subprocess.run")
    @patch("keboola_agent_cli.services.kbc_service.detect_kbc_command")
    def test_basic_export(
        self, mock_detect: MagicMock, mock_run: MagicMock, tmp_config_dir: Path, tmp_path: Path
    ) -> None:
        mock_detect.return_value = "/usr/local/bin/kbc"
        mock_run.return_value = MagicMock(returncode=0)

        store = setup_single_project(tmp_config_dir)
        svc = KbcService(config_store=store)

        with patch("keboola_agent_cli.services.kbc_service.Path.cwd", return_value=tmp_path):
            exit_code = svc.run_llm_export(alias="prod")

        assert exit_code == 0
        # Verify the command was built correctly
        cmd = mock_run.call_args[0][0]
        assert cmd[0] == "/usr/local/bin/kbc"
        assert cmd[1:3] == ["llm", "export"]
        assert "--storage-api-host" in cmd
        assert "connection.keboola.com" in cmd
        assert "--force" in cmd
        assert "--non-interactive" in cmd
        assert "--version-check=false" in cmd
        assert "--with-samples" not in cmd

    @patch("keboola_agent_cli.services.kbc_service.subprocess.run")
    @patch("keboola_agent_cli.services.kbc_service.detect_kbc_command")
    def test_export_with_samples(
        self, mock_detect: MagicMock, mock_run: MagicMock, tmp_config_dir: Path, tmp_path: Path
    ) -> None:
        mock_detect.return_value = "/usr/local/bin/kbc"
        mock_run.return_value = MagicMock(returncode=0)

        store = setup_single_project(tmp_config_dir)
        svc = KbcService(config_store=store)

        with patch("keboola_agent_cli.services.kbc_service.Path.cwd", return_value=tmp_path):
            exit_code = svc.run_llm_export(
                alias="prod", with_samples=True, sample_limit=50, max_samples=10
            )

        assert exit_code == 0
        cmd = mock_run.call_args[0][0]
        assert "--with-samples" in cmd
        assert "--sample-limit" in cmd
        assert "50" in cmd
        assert "--max-samples" in cmd
        assert "10" in cmd

    @patch("keboola_agent_cli.services.kbc_service.detect_kbc_command")
    def test_multiple_projects_requires_alias(
        self, mock_detect: MagicMock, tmp_config_dir: Path
    ) -> None:
        mock_detect.return_value = "/usr/local/bin/kbc"
        store = setup_two_projects(tmp_config_dir)
        svc = KbcService(config_store=store)

        with pytest.raises(ConfigError, match="exactly one project"):
            svc.run_llm_export()

    @patch("keboola_agent_cli.services.kbc_service.subprocess.run")
    @patch("keboola_agent_cli.services.kbc_service.detect_kbc_command")
    def test_export_nonzero_exit_code(
        self, mock_detect: MagicMock, mock_run: MagicMock, tmp_config_dir: Path, tmp_path: Path
    ) -> None:
        mock_detect.return_value = "/usr/local/bin/kbc"
        mock_run.return_value = MagicMock(returncode=1)

        store = setup_single_project(tmp_config_dir)
        svc = KbcService(config_store=store)

        with patch("keboola_agent_cli.services.kbc_service.Path.cwd", return_value=tmp_path):
            exit_code = svc.run_llm_export(alias="prod")

        assert exit_code == 1

    @patch("keboola_agent_cli.services.kbc_service.subprocess.run")
    @patch("keboola_agent_cli.services.kbc_service.detect_kbc_command")
    def test_output_dir_created(
        self, mock_detect: MagicMock, mock_run: MagicMock, tmp_config_dir: Path, tmp_path: Path
    ) -> None:
        mock_detect.return_value = "/usr/local/bin/kbc"
        mock_run.return_value = MagicMock(returncode=0)

        store = setup_single_project(tmp_config_dir)
        svc = KbcService(config_store=store)

        with patch("keboola_agent_cli.services.kbc_service.Path.cwd", return_value=tmp_path):
            svc.run_llm_export(alias="prod")

        # Verify working-dir was set to tmp_path/prod
        cmd = mock_run.call_args[0][0]
        working_dir_idx = cmd.index("--working-dir")
        assert cmd[working_dir_idx + 1] == str(tmp_path / "prod")
        assert (tmp_path / "prod").is_dir()

    @patch("keboola_agent_cli.services.kbc_service.subprocess.run")
    @patch("keboola_agent_cli.services.kbc_service.detect_kbc_command")
    def test_single_project_auto_resolve(
        self, mock_detect: MagicMock, mock_run: MagicMock, tmp_config_dir: Path, tmp_path: Path
    ) -> None:
        """When only one project exists, alias=None should auto-resolve."""
        mock_detect.return_value = "/usr/local/bin/kbc"
        mock_run.return_value = MagicMock(returncode=0)

        store = setup_single_project(tmp_config_dir)
        svc = KbcService(config_store=store)

        with patch("keboola_agent_cli.services.kbc_service.Path.cwd", return_value=tmp_path):
            exit_code = svc.run_llm_export()  # No alias

        assert exit_code == 0
