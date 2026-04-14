"""Tests for config rename feature (API rename + local sync directory rename).

Covers ConfigService.rename_config and ConfigService._rename_sync_directory.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from helpers import setup_single_project
from keboola_agent_cli.errors import KeboolaApiError
from keboola_agent_cli.services.config_service import ConfigService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_CONFIG_DETAIL = {
    "id": "cfg-001",
    "name": "Old Name",
    "description": "A test configuration",
    "configuration": {"parameters": {"key": "value"}},
}


def _make_service(
    tmp_config_dir: Path,
) -> tuple[ConfigService, MagicMock]:
    """Create a ConfigService with a mock client for rename tests."""
    store = setup_single_project(tmp_config_dir)
    mock_client = MagicMock()
    mock_client.get_config_detail.return_value = SAMPLE_CONFIG_DETAIL
    mock_client.update_config.return_value = {
        "id": "cfg-001",
        "name": "New Name",
        "componentId": "keboola.ex-http",
    }
    service = ConfigService(
        config_store=store,
        client_factory=lambda url, token: mock_client,
    )
    return service, mock_client


def _create_sync_directory(tmp_path: Path) -> Path:
    """Create a realistic Keboola CLI sync directory structure.

    Returns the project root directory (parent of .keboola/).
    """
    keboola_dir = tmp_path / ".keboola"
    keboola_dir.mkdir(parents=True)

    manifest_data = {
        "version": 2,
        "project": {"id": 258, "apiHost": "connection.keboola.com"},
        "allowTargetEnv": True,
        "gitBranching": {"enabled": False, "defaultBranch": "main"},
        "sortBy": "id",
        "naming": {
            "branch": "{branch_name}",
            "config": "{component_type}/{component_id}/{config_name}",
            "configRow": "rows/{config_row_name}",
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
                "metadata": {"pull_hash": "abc", "pull_config_hash": "def"},
                "rows": [],
            }
        ],
    }
    (keboola_dir / "manifest.json").write_text(json.dumps(manifest_data))

    # Create the actual config directory with a file inside
    config_dir = tmp_path / "main" / "extractor" / "keboola.ex-http" / "old-name"
    config_dir.mkdir(parents=True)
    (config_dir / "_config.yml").write_text("name: Old Name\n")

    return tmp_path


# ---------------------------------------------------------------------------
# rename_config (API-level) tests
# ---------------------------------------------------------------------------


class TestRenameConfigApi:
    """Tests for ConfigService.rename_config API interaction."""

    def test_rename_config_basic(self, tmp_config_dir: Path) -> None:
        """Rename via API returns old_name and new_name in result."""
        service, client = _make_service(tmp_config_dir)

        result = service.rename_config(
            alias="prod",
            component_id="keboola.ex-http",
            config_id="cfg-001",
            name="New Name",
        )

        assert result["status"] == "renamed"
        assert result["old_name"] == "Old Name"
        assert result["new_name"] == "New Name"
        assert result["project_alias"] == "prod"
        assert result["component_id"] == "keboola.ex-http"
        assert result["config_id"] == "cfg-001"

        client.get_config_detail.assert_called_once_with(
            "keboola.ex-http", "cfg-001", branch_id=None
        )
        client.update_config.assert_called_once()
        call_kwargs = client.update_config.call_args.kwargs
        assert call_kwargs["name"] == "New Name"
        assert call_kwargs["component_id"] == "keboola.ex-http"
        assert call_kwargs["config_id"] == "cfg-001"

    def test_rename_config_with_branch(self, tmp_config_dir: Path) -> None:
        """Rename with branch_id passes it through to API calls."""
        service, client = _make_service(tmp_config_dir)

        result = service.rename_config(
            alias="prod",
            component_id="keboola.ex-http",
            config_id="cfg-001",
            name="New Name",
            branch_id=9999,
        )

        assert result["branch_id"] == 9999

        client.get_config_detail.assert_called_once_with(
            "keboola.ex-http", "cfg-001", branch_id=9999
        )
        call_kwargs = client.update_config.call_args.kwargs
        assert call_kwargs["branch_id"] == 9999

    def test_rename_config_api_error(self, tmp_config_dir: Path) -> None:
        """KeboolaApiError from the client propagates to the caller."""
        service, client = _make_service(tmp_config_dir)
        client.get_config_detail.side_effect = KeboolaApiError(
            status_code=404,
            message="Configuration not found",
        )

        with pytest.raises(KeboolaApiError, match="Configuration not found"):
            service.rename_config(
                alias="prod",
                component_id="keboola.ex-http",
                config_id="cfg-001",
                name="New Name",
            )


# ---------------------------------------------------------------------------
# _rename_sync_directory tests
# ---------------------------------------------------------------------------


class TestRenameSyncDirectory:
    """Tests for ConfigService._rename_sync_directory."""

    def test_rename_sync_directory_no_directory(self, tmp_config_dir: Path) -> None:
        """Returns None when directory is None."""
        service, _ = _make_service(tmp_config_dir)

        result = service._rename_sync_directory(
            directory=None,
            component_id="keboola.ex-http",
            config_id="cfg-001",
            new_name="New Name",
        )

        assert result is None

    def test_rename_sync_directory_no_manifest(self, tmp_config_dir: Path, tmp_path: Path) -> None:
        """Returns None when no manifest.json exists in the directory."""
        service, _ = _make_service(tmp_config_dir)
        empty_dir = tmp_path / "no-manifest"
        empty_dir.mkdir()

        result = service._rename_sync_directory(
            directory=empty_dir,
            component_id="keboola.ex-http",
            config_id="cfg-001",
            new_name="New Name",
        )

        assert result is None

    def test_rename_sync_directory_config_not_tracked(
        self, tmp_config_dir: Path, tmp_path: Path
    ) -> None:
        """Returns None when the config is not tracked in the manifest."""
        service, _ = _make_service(tmp_config_dir)
        project_root = _create_sync_directory(tmp_path / "sync")

        result = service._rename_sync_directory(
            directory=project_root,
            component_id="keboola.ex-http",
            config_id="cfg-999",  # Not in manifest
            new_name="New Name",
        )

        assert result is None

    def test_rename_sync_directory_no_change_needed(
        self, tmp_config_dir: Path, tmp_path: Path
    ) -> None:
        """Returns None when old name matches new name after sanitization."""
        service, _ = _make_service(tmp_config_dir)
        project_root = _create_sync_directory(tmp_path / "sync")

        # "old-name" sanitized stays "old-name"
        result = service._rename_sync_directory(
            directory=project_root,
            component_id="keboola.ex-http",
            config_id="cfg-001",
            new_name="Old Name",  # sanitize_name("Old Name") == "old-name"
        )

        assert result is None

    def test_rename_sync_directory_renames_dir(self, tmp_config_dir: Path, tmp_path: Path) -> None:
        """Full rename: moves files, updates manifest path, clears hashes."""
        service, _ = _make_service(tmp_config_dir)
        project_root = _create_sync_directory(tmp_path / "sync")

        result = service._rename_sync_directory(
            directory=project_root,
            component_id="keboola.ex-http",
            config_id="cfg-001",
            new_name="New Name",
        )

        assert result is not None
        assert result["old_path"] == "extractor/keboola.ex-http/old-name"
        assert result["new_path"] == "extractor/keboola.ex-http/new-name"
        assert result["method"] in ("git_mv", "shutil_move")

        # Verify old directory is gone and new one exists
        old_dir = project_root / "main" / "extractor" / "keboola.ex-http" / "old-name"
        new_dir = project_root / "main" / "extractor" / "keboola.ex-http" / "new-name"
        assert not old_dir.exists()
        assert new_dir.exists()
        assert (new_dir / "_config.yml").read_text() == "name: Old Name\n"

        # Verify manifest was updated
        manifest_path = project_root / ".keboola" / "manifest.json"
        manifest_data = json.loads(manifest_path.read_text())
        cfg_entry = manifest_data["configurations"][0]
        assert cfg_entry["path"] == "extractor/keboola.ex-http/new-name"
        # Pull hashes should be cleared
        assert "pull_hash" not in cfg_entry.get("metadata", {})
        assert "pull_config_hash" not in cfg_entry.get("metadata", {})

    def test_rename_sync_directory_collision(self, tmp_config_dir: Path, tmp_path: Path) -> None:
        """When target dir already exists, appends a numeric suffix."""
        service, _ = _make_service(tmp_config_dir)
        project_root = _create_sync_directory(tmp_path / "sync")

        # Pre-create the target directory to cause a collision
        collision_dir = project_root / "main" / "extractor" / "keboola.ex-http" / "new-name"
        collision_dir.mkdir(parents=True)
        (collision_dir / "_config.yml").write_text("name: Existing\n")

        result = service._rename_sync_directory(
            directory=project_root,
            component_id="keboola.ex-http",
            config_id="cfg-001",
            new_name="New Name",
        )

        assert result is not None
        # Should get a numeric suffix due to collision
        assert result["new_path"] == "extractor/keboola.ex-http/new-name-2"

        # Original collision dir is untouched
        assert collision_dir.exists()
        assert (collision_dir / "_config.yml").read_text() == "name: Existing\n"

        # New suffixed dir exists with moved content
        suffixed_dir = project_root / "main" / "extractor" / "keboola.ex-http" / "new-name-2"
        assert suffixed_dir.exists()
        assert (suffixed_dir / "_config.yml").read_text() == "name: Old Name\n"

    def test_rename_sync_directory_source_missing(
        self, tmp_config_dir: Path, tmp_path: Path
    ) -> None:
        """When source dir doesn't exist, updates manifest only."""
        service, _ = _make_service(tmp_config_dir)
        project_root = _create_sync_directory(tmp_path / "sync")

        # Remove the source directory (simulates not-yet-pulled state)
        source_dir = project_root / "main" / "extractor" / "keboola.ex-http" / "old-name"
        import shutil

        shutil.rmtree(source_dir)

        result = service._rename_sync_directory(
            directory=project_root,
            component_id="keboola.ex-http",
            config_id="cfg-001",
            new_name="New Name",
        )

        assert result is not None
        assert result["old_path"] == "extractor/keboola.ex-http/old-name"
        assert result["new_path"] == "extractor/keboola.ex-http/new-name"
        assert result["method"] == "manifest_only"

        # Verify manifest was updated
        manifest_path = project_root / ".keboola" / "manifest.json"
        manifest_data = json.loads(manifest_path.read_text())
        cfg_entry = manifest_data["configurations"][0]
        assert cfg_entry["path"] == "extractor/keboola.ex-http/new-name"
        assert "pull_hash" not in cfg_entry.get("metadata", {})
        assert "pull_config_hash" not in cfg_entry.get("metadata", {})
