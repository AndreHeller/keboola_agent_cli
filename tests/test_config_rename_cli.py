"""Tests for config rename CLI command via CliRunner.

Tests the `kbagent config rename` subcommand: JSON output, human-readable
output, sync directory info, API error handling, and help text.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from helpers import setup_single_project
from keboola_agent_cli.cli import app
from keboola_agent_cli.errors import KeboolaApiError
from keboola_agent_cli.services.config_service import ConfigService

runner = CliRunner()


class TestConfigRenameCli:
    """Tests for `kbagent config rename` command."""

    def test_config_rename_json_output(self, tmp_config_dir: Path) -> None:
        """config rename --json returns structured JSON with rename details."""
        store = setup_single_project(tmp_config_dir)

        mock_client = MagicMock()
        mock_client.get_config_detail.return_value = {
            "id": "cfg-001",
            "name": "Old Name",
            "componentId": "keboola.ex-http",
        }
        mock_client.update_config.return_value = {
            "id": "cfg-001",
            "name": "New Name",
            "componentId": "keboola.ex-http",
        }

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "keboola_agent_cli.commands.config.get_service",
                lambda ctx, name: ConfigService(
                    config_store=store,
                    client_factory=lambda url, token: mock_client,
                ),
            )

            result = runner.invoke(
                app,
                [
                    "--json",
                    "--config-dir",
                    str(tmp_config_dir),
                    "config",
                    "rename",
                    "--project",
                    "prod",
                    "--component-id",
                    "keboola.ex-http",
                    "--config-id",
                    "cfg-001",
                    "--name",
                    "New Name",
                ],
            )

        assert result.exit_code == 0, f"Exit code {result.exit_code}: {result.output}"
        output = json.loads(result.output)
        assert output["status"] == "ok"
        assert output["data"]["status"] == "renamed"
        assert output["data"]["old_name"] == "Old Name"
        assert output["data"]["new_name"] == "New Name"
        assert output["data"]["component_id"] == "keboola.ex-http"
        assert output["data"]["config_id"] == "cfg-001"
        assert output["data"]["project_alias"] == "prod"

    def test_config_rename_human_output(self, tmp_config_dir: Path) -> None:
        """config rename in human mode outputs success message with rename info."""
        store = setup_single_project(tmp_config_dir)

        mock_client = MagicMock()
        mock_client.get_config_detail.return_value = {
            "id": "cfg-001",
            "name": "Old Name",
            "componentId": "keboola.ex-http",
        }
        mock_client.update_config.return_value = {
            "id": "cfg-001",
            "name": "New Name",
            "componentId": "keboola.ex-http",
        }

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "keboola_agent_cli.commands.config.get_service",
                lambda ctx, name: ConfigService(
                    config_store=store,
                    client_factory=lambda url, token: mock_client,
                ),
            )

            result = runner.invoke(
                app,
                [
                    "--config-dir",
                    str(tmp_config_dir),
                    "config",
                    "rename",
                    "--project",
                    "prod",
                    "--component-id",
                    "keboola.ex-http",
                    "--config-id",
                    "cfg-001",
                    "--name",
                    "New Name",
                ],
            )

        assert result.exit_code == 0, f"Exit code {result.exit_code}: {result.output}"
        assert "Renamed" in result.output
        assert "Old Name" in result.output
        assert "New Name" in result.output
        assert "keboola.ex-http" in result.output

    def test_config_rename_with_sync_info(self, tmp_config_dir: Path, tmp_path: Path) -> None:
        """config rename with sync directory shows sync rename details in human output."""
        store = setup_single_project(tmp_config_dir)

        mock_client = MagicMock()
        mock_client.get_config_detail.return_value = {
            "id": "cfg-001",
            "name": "Old Name",
            "componentId": "keboola.ex-http",
        }
        mock_client.update_config.return_value = {
            "id": "cfg-001",
            "name": "New Name",
            "componentId": "keboola.ex-http",
        }

        # Set up a mock sync directory with manifest
        sync_dir = tmp_path / "sync_project"
        sync_dir.mkdir()
        keboola_dir = sync_dir / ".keboola"
        keboola_dir.mkdir()
        manifest = {
            "version": 2,
            "project": {"id": 258, "apiHost": "connection.keboola.com"},
            "allowTargetEnv": True,
            "gitBranching": {"enabled": False, "defaultBranch": "main"},
            "sortBy": "id",
            "naming": {
                "branch": "{branch_name}",
                "config": "{component_type}/{component_id}/{config_name}",
                "configRow": "rows/{config_row_name}",
                "schedulerConfig": "schedules/{config_name}",
                "sharedCodeConfig": "_shared/{target_component_id}",
                "sharedCodeConfigRow": "codes/{config_row_name}",
                "variablesConfig": "variables",
                "variablesValuesRow": "values/{config_row_name}",
                "dataAppConfig": "app/{component_id}/{config_name}",
            },
            "allowedBranches": [],
            "ignoredComponents": [],
            "branches": [{"id": 12345, "path": "main", "metadata": {}}],
            "configurations": [
                {
                    "branchId": 12345,
                    "componentId": "keboola.ex-http",
                    "id": "cfg-001",
                    "path": "extractor/keboola.ex-http/old-name",
                    "metadata": {},
                    "rows": [],
                }
            ],
        }
        (keboola_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

        # Create the old config directory on disk
        old_config_dir = sync_dir / "main" / "extractor" / "keboola.ex-http" / "old-name"
        old_config_dir.mkdir(parents=True)
        (old_config_dir / "_config.yml").write_text("name: Old Name\n", encoding="utf-8")

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "keboola_agent_cli.commands.config.get_service",
                lambda ctx, name: ConfigService(
                    config_store=store,
                    client_factory=lambda url, token: mock_client,
                ),
            )

            result = runner.invoke(
                app,
                [
                    "--config-dir",
                    str(tmp_config_dir),
                    "config",
                    "rename",
                    "--project",
                    "prod",
                    "--component-id",
                    "keboola.ex-http",
                    "--config-id",
                    "cfg-001",
                    "--name",
                    "New Name",
                    "--directory",
                    str(sync_dir),
                ],
            )

        assert result.exit_code == 0, f"Exit code {result.exit_code}: {result.output}"
        assert "Renamed" in result.output
        # The sync info line shows old and new paths
        assert "Sync:" in result.output

    def test_config_rename_api_error(self, tmp_config_dir: Path) -> None:
        """config rename with API error returns appropriate exit code."""
        store = setup_single_project(tmp_config_dir)

        mock_client = MagicMock()
        mock_client.get_config_detail.side_effect = KeboolaApiError(
            message="Configuration 'cfg-999' not found",
            status_code=404,
            error_code="NOT_FOUND",
            retryable=False,
        )

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "keboola_agent_cli.commands.config.get_service",
                lambda ctx, name: ConfigService(
                    config_store=store,
                    client_factory=lambda url, token: mock_client,
                ),
            )

            result = runner.invoke(
                app,
                [
                    "--json",
                    "--config-dir",
                    str(tmp_config_dir),
                    "config",
                    "rename",
                    "--project",
                    "prod",
                    "--component-id",
                    "keboola.ex-http",
                    "--config-id",
                    "cfg-999",
                    "--name",
                    "Whatever",
                ],
            )

        # NOT_FOUND maps to exit code 1 (general error)
        assert result.exit_code == 1
        output = json.loads(result.output)
        assert output["status"] == "error"
        assert "NOT_FOUND" in output["error"]["code"]

    def test_config_rename_help(self) -> None:
        """config rename --help shows usage information."""
        result = runner.invoke(app, ["config", "rename", "--help"])

        assert result.exit_code == 0
        assert "Rename a configuration" in result.output
        assert "--project" in result.output
        assert "--component-id" in result.output
        assert "--config-id" in result.output
        assert "--name" in result.output
        assert "--directory" in result.output
