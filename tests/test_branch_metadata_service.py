"""Tests for BranchService branch-metadata and project-description methods."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from helpers import setup_single_project
from keboola_agent_cli.errors import ConfigError, KeboolaApiError
from keboola_agent_cli.services.branch_service import (
    PROJECT_DESCRIPTION_KEY,
    BranchService,
)

SAMPLE_ENTRIES = [
    {"id": 1, "key": "b-key", "value": "bv", "provider": "user"},
    {"id": 2, "key": "a-key", "value": "av", "provider": "user"},
    {"id": 3, "key": PROJECT_DESCRIPTION_KEY, "value": "# Hello", "provider": "user"},
]


class TestBranchMetadataList:
    def test_list_returns_sorted_by_key(self, tmp_config_dir: Path) -> None:
        mock_client = MagicMock()
        mock_client.list_branch_metadata.return_value = SAMPLE_ENTRIES
        store = setup_single_project(tmp_config_dir)
        svc = BranchService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        result = svc.list_branch_metadata(alias="prod")

        assert result["project_alias"] == "prod"
        assert result["branch_id"] == "default"
        # Sorted lexicographically; uppercase "KBC." sorts before lowercase.
        assert [e["key"] for e in result["metadata"]] == [
            PROJECT_DESCRIPTION_KEY,
            "a-key",
            "b-key",
        ]
        mock_client.list_branch_metadata.assert_called_once_with(branch_id="default")
        mock_client.close.assert_called_once()

    def test_list_numeric_branch_id(self, tmp_config_dir: Path) -> None:
        mock_client = MagicMock()
        mock_client.list_branch_metadata.return_value = []
        store = setup_single_project(tmp_config_dir)
        svc = BranchService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        svc.list_branch_metadata(alias="prod", branch_id=456)

        mock_client.list_branch_metadata.assert_called_once_with(branch_id=456)

    def test_list_missing_alias_raises(self, tmp_config_dir: Path) -> None:
        store = setup_single_project(tmp_config_dir)
        svc = BranchService(
            config_store=store,
            client_factory=lambda url, token: MagicMock(),
        )
        with pytest.raises(ConfigError):
            svc.list_branch_metadata(alias="does-not-exist")


class TestBranchMetadataGet:
    def test_get_returns_value(self, tmp_config_dir: Path) -> None:
        mock_client = MagicMock()
        mock_client.get_branch_metadata_value.return_value = "# Hello"
        store = setup_single_project(tmp_config_dir)
        svc = BranchService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        result = svc.get_branch_metadata(alias="prod", key=PROJECT_DESCRIPTION_KEY)

        assert result["value"] == "# Hello"
        assert result["key"] == PROJECT_DESCRIPTION_KEY
        assert result["branch_id"] == "default"
        mock_client.close.assert_called_once()

    def test_get_missing_key_raises_not_found(self, tmp_config_dir: Path) -> None:
        mock_client = MagicMock()
        mock_client.get_branch_metadata_value.return_value = None
        store = setup_single_project(tmp_config_dir)
        svc = BranchService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        with pytest.raises(KeboolaApiError) as exc_info:
            svc.get_branch_metadata(alias="prod", key="missing")

        assert exc_info.value.error_code == "NOT_FOUND"
        assert exc_info.value.status_code == 404
        mock_client.close.assert_called_once()


class TestBranchMetadataSet:
    def test_set_forwards_entries(self, tmp_config_dir: Path) -> None:
        mock_client = MagicMock()
        mock_client.set_branch_metadata.return_value = [SAMPLE_ENTRIES[2]]
        store = setup_single_project(tmp_config_dir)
        svc = BranchService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        result = svc.set_branch_metadata(alias="prod", key="k", value="v", branch_id="default")

        mock_client.set_branch_metadata.assert_called_once_with(
            entries=[("k", "v")], branch_id="default"
        )
        assert result["key"] == "k"
        assert result["value"] == "v"
        assert "set on branch" in result["message"]


class TestBranchMetadataDelete:
    def test_delete_forwards_metadata_id(self, tmp_config_dir: Path) -> None:
        mock_client = MagicMock()
        store = setup_single_project(tmp_config_dir)
        svc = BranchService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        result = svc.delete_branch_metadata(alias="prod", metadata_id=99)

        mock_client.delete_branch_metadata.assert_called_once_with(
            metadata_id=99, branch_id="default"
        )
        assert result["metadata_id"] == 99
        assert "deleted" in result["message"]


class TestProjectDescription:
    def test_get_returns_description(self, tmp_config_dir: Path) -> None:
        mock_client = MagicMock()
        mock_client.get_branch_metadata_value.return_value = "# My project"
        store = setup_single_project(tmp_config_dir)
        svc = BranchService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        result = svc.get_project_description(alias="prod")

        assert result["description"] == "# My project"
        assert result["key"] == PROJECT_DESCRIPTION_KEY
        mock_client.get_branch_metadata_value.assert_called_once_with(
            key=PROJECT_DESCRIPTION_KEY, branch_id="default"
        )

    def test_get_returns_empty_when_unset(self, tmp_config_dir: Path) -> None:
        """A missing project description returns '', not an error."""
        mock_client = MagicMock()
        mock_client.get_branch_metadata_value.return_value = None
        store = setup_single_project(tmp_config_dir)
        svc = BranchService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        result = svc.get_project_description(alias="prod")

        assert result["description"] == ""

    def test_set_forwards_to_set_branch_metadata(self, tmp_config_dir: Path) -> None:
        mock_client = MagicMock()
        mock_client.set_branch_metadata.return_value = [SAMPLE_ENTRIES[2]]
        store = setup_single_project(tmp_config_dir)
        svc = BranchService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        description = "# My new description"
        result = svc.set_project_description(alias="prod", description=description)

        mock_client.set_branch_metadata.assert_called_once_with(
            entries=[(PROJECT_DESCRIPTION_KEY, description)], branch_id="default"
        )
        assert result["description"] == description
        assert str(len(description)) in result["message"]
