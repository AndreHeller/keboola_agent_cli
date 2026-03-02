"""Tests for BranchService - multi-project development branch listing."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from helpers import setup_single_project, setup_two_projects
from keboola_agent_cli.errors import ConfigError, KeboolaApiError
from keboola_agent_cli.services.branch_service import BranchService

SAMPLE_BRANCHES = [
    {
        "id": 123,
        "name": "main",
        "isDefault": True,
        "created": "2025-01-01T00:00:00Z",
        "description": "Main branch",
    },
    {
        "id": 456,
        "name": "feature-x",
        "isDefault": False,
        "created": "2025-06-15T10:30:00Z",
        "description": "Feature branch",
    },
]

SAMPLE_BRANCHES_DEV = [
    {
        "id": 789,
        "name": "main",
        "isDefault": True,
        "created": "2025-02-01T00:00:00Z",
        "description": "Dev main branch",
    },
]


class TestListBranchesSingleProject:
    """Tests for BranchService.list_branches() with a single project."""

    def test_list_branches_single_project(self, tmp_config_dir: Path) -> None:
        """list_branches returns branches annotated with project alias."""
        mock_client = MagicMock()
        mock_client.list_dev_branches.return_value = SAMPLE_BRANCHES

        store = setup_single_project(tmp_config_dir)
        svc = BranchService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        result = svc.list_branches(aliases=["prod"])
        branches = result["branches"]
        errors = result["errors"]

        assert errors == []
        assert len(branches) == 2
        assert branches[0]["project_alias"] == "prod"
        assert branches[0]["id"] == 123
        assert branches[0]["name"] == "main"
        assert branches[0]["isDefault"] is True
        assert branches[1]["id"] == 456
        assert branches[1]["name"] == "feature-x"
        assert branches[1]["isDefault"] is False

    def test_list_branches_empty(self, tmp_config_dir: Path) -> None:
        """list_branches returns empty list when project has no dev branches."""
        mock_client = MagicMock()
        mock_client.list_dev_branches.return_value = []

        store = setup_single_project(tmp_config_dir)
        svc = BranchService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        result = svc.list_branches(aliases=["prod"])
        assert result["branches"] == []
        assert result["errors"] == []


class TestListBranchesMultiProject:
    """Tests for BranchService.list_branches() with multiple projects."""

    def test_list_branches_multi_project(self, tmp_config_dir: Path) -> None:
        """list_branches aggregates branches from all projects."""
        call_count = 0

        def make_client(url: str, token: str) -> MagicMock:
            nonlocal call_count
            call_count += 1
            mock = MagicMock()
            if token == "901-xxx":
                mock.list_dev_branches.return_value = SAMPLE_BRANCHES
            else:
                mock.list_dev_branches.return_value = SAMPLE_BRANCHES_DEV
            return mock

        store = setup_two_projects(tmp_config_dir)
        svc = BranchService(
            config_store=store,
            client_factory=make_client,
        )

        result = svc.list_branches()
        branches = result["branches"]
        errors = result["errors"]

        assert errors == []
        assert len(branches) == 3

        # Branches should be sorted by (project_alias, id)
        dev_branches = [b for b in branches if b["project_alias"] == "dev"]
        prod_branches = [b for b in branches if b["project_alias"] == "prod"]
        assert len(dev_branches) == 1
        assert len(prod_branches) == 2


class TestListBranchesWithError:
    """Tests for error handling in BranchService.list_branches()."""

    def test_list_branches_one_project_fails(self, tmp_config_dir: Path) -> None:
        """When one project fails, the other still returns results."""
        def make_client(url: str, token: str) -> MagicMock:
            mock = MagicMock()
            if token == "901-xxx":
                mock.list_dev_branches.return_value = SAMPLE_BRANCHES
            else:
                mock.list_dev_branches.side_effect = KeboolaApiError(
                    message="Connection refused",
                    error_code="CONNECTION_ERROR",
                    status_code=0,
                    retryable=True,
                )
            return mock

        store = setup_two_projects(tmp_config_dir)
        svc = BranchService(
            config_store=store,
            client_factory=make_client,
        )

        result = svc.list_branches()
        branches = result["branches"]
        errors = result["errors"]

        assert len(branches) == 2
        assert all(b["project_alias"] == "prod" for b in branches)
        assert len(errors) == 1
        assert errors[0]["project_alias"] == "dev"
        assert errors[0]["error_code"] == "CONNECTION_ERROR"

    def test_list_branches_unknown_alias_raises(self, tmp_config_dir: Path) -> None:
        """Specifying a nonexistent alias raises ConfigError."""
        store = setup_single_project(tmp_config_dir)
        svc = BranchService(config_store=store)

        with pytest.raises(ConfigError, match="Project 'nonexistent' not found"):
            svc.list_branches(aliases=["nonexistent"])

    def test_list_branches_unexpected_error(self, tmp_config_dir: Path) -> None:
        """Unexpected exceptions are captured as UNEXPECTED_ERROR."""
        mock_client = MagicMock()
        mock_client.list_dev_branches.side_effect = RuntimeError("Something broke")

        store = setup_single_project(tmp_config_dir)
        svc = BranchService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        result = svc.list_branches(aliases=["prod"])
        assert result["branches"] == []
        assert len(result["errors"]) == 1
        assert result["errors"][0]["error_code"] == "UNEXPECTED_ERROR"
        assert "Something broke" in result["errors"][0]["message"]
