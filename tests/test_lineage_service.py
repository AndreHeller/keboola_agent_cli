"""Tests for LineageService - cross-project data lineage via bucket sharing."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from keboola_agent_cli.config_store import ConfigStore
from keboola_agent_cli.errors import ConfigError, KeboolaApiError
from keboola_agent_cli.models import ProjectConfig
from keboola_agent_cli.services.lineage_service import LineageService


def _make_lineage_client(buckets: list[dict]) -> MagicMock:
    """Create a mock KeboolaClient that returns the given list of buckets."""
    mock_client = MagicMock()
    mock_client.list_buckets.return_value = buckets
    return mock_client


def _make_failing_lineage_client(error: KeboolaApiError) -> MagicMock:
    """Create a mock KeboolaClient whose list_buckets raises the given error."""
    mock_client = MagicMock()
    mock_client.list_buckets.side_effect = error
    return mock_client


def _setup_single_project(
    tmp_config_dir: Path,
    alias: str = "prod",
    stack_url: str = "https://connection.keboola.com",
    token: str = "901-xxx",
    project_name: str = "Production",
    project_id: int = 258,
) -> ConfigStore:
    """Create a ConfigStore with a single project configured."""
    store = ConfigStore(config_dir=tmp_config_dir)
    store.add_project(
        alias,
        ProjectConfig(
            stack_url=stack_url,
            token=token,
            project_name=project_name,
            project_id=project_id,
        ),
    )
    return store


def _setup_two_projects(tmp_config_dir: Path) -> ConfigStore:
    """Create a ConfigStore with two projects (prod and dev) configured."""
    store = ConfigStore(config_dir=tmp_config_dir)
    store.add_project(
        "prod",
        ProjectConfig(
            stack_url="https://connection.keboola.com",
            token="901-xxx",
            project_name="Production",
            project_id=258,
        ),
    )
    store.add_project(
        "dev",
        ProjectConfig(
            stack_url="https://connection.keboola.com",
            token="7012-yyy",
            project_name="Development",
            project_id=7012,
        ),
    )
    return store


class TestLineageSharedBucket:
    """Tests for a project with a shared bucket that has linkedBy entries."""

    def test_shared_bucket_with_linked_by(self, tmp_config_dir: Path) -> None:
        """A shared bucket with linkedBy entries produces edges and shared_buckets."""
        store = _setup_single_project(tmp_config_dir)

        buckets = [
            {
                "id": "in.c-shared-data",
                "name": "shared-data",
                "sharing": "organization-project",
                "sharedBy": {"projectId": 258, "projectName": "Production"},
                "linkedBy": [
                    {
                        "project": {"id": 7012, "name": "Target"},
                        "id": "in.c-linked",
                    }
                ],
            },
        ]
        mock_client = _make_lineage_client(buckets)

        service = LineageService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        result = service.get_lineage(aliases=["prod"])

        # Verify shared buckets
        assert len(result["shared_buckets"]) == 1
        shared = result["shared_buckets"][0]
        assert shared["project_alias"] == "prod"
        assert shared["project_id"] == 258
        assert shared["bucket_id"] == "in.c-shared-data"
        assert shared["sharing_type"] == "organization-project"

        # Verify edges
        assert len(result["edges"]) == 1
        edge = result["edges"][0]
        assert edge["source_project_id"] == 258
        assert edge["source_project_alias"] == "prod"
        assert edge["source_bucket_id"] == "in.c-shared-data"
        assert edge["target_project_id"] == 7012
        assert edge["target_project_name"] == "Target"
        assert edge["target_bucket_id"] == "in.c-linked"
        assert edge["sharing_type"] == "organization-project"

        # Verify no linked buckets (this project shares, not links)
        assert len(result["linked_buckets"]) == 0

        # Verify summary
        assert result["summary"]["total_shared_buckets"] == 1
        assert result["summary"]["total_linked_buckets"] == 0
        assert result["summary"]["total_edges"] == 1
        assert result["summary"]["projects_queried"] == 1
        assert result["summary"]["projects_with_errors"] == 0

    def test_shared_bucket_multiple_linked_by(self, tmp_config_dir: Path) -> None:
        """A shared bucket linked by multiple projects produces multiple edges."""
        store = _setup_single_project(tmp_config_dir)

        buckets = [
            {
                "id": "in.c-shared-data",
                "name": "shared-data",
                "sharing": "organization-project",
                "linkedBy": [
                    {"project": {"id": 7012, "name": "Target A"}, "id": "in.c-linked-a"},
                    {"project": {"id": 8888, "name": "Target B"}, "id": "in.c-linked-b"},
                ],
            },
        ]
        mock_client = _make_lineage_client(buckets)

        service = LineageService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        result = service.get_lineage(aliases=["prod"])

        assert len(result["edges"]) == 2
        target_ids = {e["target_project_id"] for e in result["edges"]}
        assert target_ids == {7012, 8888}


class TestLineageLinkedBucket:
    """Tests for a project with a linked bucket that has sourceBucket."""

    def test_linked_bucket_with_source(self, tmp_config_dir: Path) -> None:
        """A linked bucket with sourceBucket produces edges and linked_buckets."""
        store = _setup_single_project(
            tmp_config_dir,
            alias="dev",
            token="7012-yyy",
            project_name="Development",
            project_id=7012,
        )

        buckets = [
            {
                "id": "in.c-linked",
                "name": "linked",
                "isReadonly": True,
                "sourceBucket": {
                    "id": "in.c-original",
                    "project": {"id": 258, "name": "Source"},
                },
            },
        ]
        mock_client = _make_lineage_client(buckets)

        service = LineageService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        result = service.get_lineage(aliases=["dev"])

        # Verify linked buckets
        assert len(result["linked_buckets"]) == 1
        linked = result["linked_buckets"][0]
        assert linked["project_alias"] == "dev"
        assert linked["project_id"] == 7012
        assert linked["bucket_id"] == "in.c-linked"
        assert linked["source_bucket_id"] == "in.c-original"
        assert linked["source_project_id"] == 258
        assert linked["source_project_name"] == "Source"
        assert linked["is_readonly"] is True

        # Verify edges
        assert len(result["edges"]) == 1
        edge = result["edges"][0]
        assert edge["source_project_id"] == 258
        assert edge["source_project_name"] == "Source"
        assert edge["source_bucket_id"] == "in.c-original"
        assert edge["target_project_id"] == 7012
        assert edge["target_project_alias"] == "dev"
        assert edge["target_bucket_id"] == "in.c-linked"

        # Verify no shared buckets
        assert len(result["shared_buckets"]) == 0

        # Verify summary
        assert result["summary"]["total_shared_buckets"] == 0
        assert result["summary"]["total_linked_buckets"] == 1
        assert result["summary"]["total_edges"] == 1


class TestLineageMultiProjectDedup:
    """Tests for edge deduplication when both sharing and linking projects are queried."""

    def test_edges_deduplicated_across_two_projects(self, tmp_config_dir: Path) -> None:
        """When project A shares and project B links the same bucket, edges are deduplicated."""
        store = _setup_two_projects(tmp_config_dir)

        # Project A (prod, id=258) shares bucket with linkedBy pointing to project B
        prod_buckets = [
            {
                "id": "in.c-shared",
                "name": "shared",
                "sharing": "organization-project",
                "linkedBy": [
                    {"project": {"id": 7012, "name": "Development"}, "id": "in.c-linked"},
                ],
            },
        ]
        prod_client = _make_lineage_client(prod_buckets)

        # Project B (dev, id=7012) has linked bucket pointing back to project A
        dev_buckets = [
            {
                "id": "in.c-linked",
                "name": "linked",
                "sourceBucket": {
                    "id": "in.c-shared",
                    "project": {"id": 258, "name": "Production"},
                },
            },
        ]
        dev_client = _make_lineage_client(dev_buckets)

        def factory(url: str, token: str) -> MagicMock:
            if "901" in token:
                return prod_client
            return dev_client

        service = LineageService(
            config_store=store,
            client_factory=factory,
        )

        result = service.get_lineage()

        # The key point: edges should be deduplicated to exactly 1
        assert len(result["edges"]) == 1

        edge = result["edges"][0]
        # Source side comes from the shared bucket (project A / prod)
        assert edge["source_project_id"] == 258
        assert edge["source_project_alias"] == "prod"
        assert edge["source_bucket_id"] == "in.c-shared"

        # Target side comes from the linked bucket (project B / dev)
        assert edge["target_project_id"] == 7012
        assert edge["target_project_alias"] == "dev"
        assert edge["target_bucket_id"] == "in.c-linked"

        assert edge["sharing_type"] == "organization-project"

        # Both shared and linked buckets should still be recorded
        assert len(result["shared_buckets"]) == 1
        assert len(result["linked_buckets"]) == 1

        # Summary reflects deduplication
        assert result["summary"]["total_edges"] == 1
        assert result["summary"]["projects_queried"] == 2

    def test_dedup_with_same_edge_key(self, tmp_config_dir: Path) -> None:
        """Both projects report the same edge key, result is 1 edge not 2."""
        store = _setup_two_projects(tmp_config_dir)

        # Both projects produce the same edge key: (258, "in.c-shared", 7012, "in.c-linked")
        prod_buckets = [
            {
                "id": "in.c-shared",
                "name": "shared",
                "sharing": "organization-project",
                "linkedBy": [
                    {"project": {"id": 7012, "name": "Development"}, "id": "in.c-linked"},
                ],
            },
        ]
        dev_buckets = [
            {
                "id": "in.c-linked",
                "name": "linked",
                "sourceBucket": {
                    "id": "in.c-shared",
                    "project": {"id": 258, "name": "Production"},
                },
            },
        ]

        prod_client = _make_lineage_client(prod_buckets)
        dev_client = _make_lineage_client(dev_buckets)

        def factory(url: str, token: str) -> MagicMock:
            if "901" in token:
                return prod_client
            return dev_client

        service = LineageService(config_store=store, client_factory=factory)
        result = service.get_lineage()

        assert len(result["edges"]) == 1
        assert result["summary"]["total_edges"] == 1


class TestLineageErrorAccumulation:
    """Tests for error handling when one or more projects fail."""

    def test_one_project_fails_other_succeeds(self, tmp_config_dir: Path) -> None:
        """When one project fails with KeboolaApiError, the other still produces results."""
        store = _setup_two_projects(tmp_config_dir)

        # prod client will fail
        error = KeboolaApiError(
            message="Forbidden",
            status_code=403,
            error_code="ACCESS_DENIED",
            retryable=False,
        )
        prod_client = _make_failing_lineage_client(error)

        # dev client returns a linked bucket
        dev_buckets = [
            {
                "id": "in.c-linked",
                "name": "linked",
                "sourceBucket": {
                    "id": "in.c-shared",
                    "project": {"id": 258, "name": "Production"},
                },
            },
        ]
        dev_client = _make_lineage_client(dev_buckets)

        def factory(url: str, token: str) -> MagicMock:
            if "901" in token:
                return prod_client
            return dev_client

        service = LineageService(config_store=store, client_factory=factory)
        result = service.get_lineage()

        # Errors list should contain the failed project
        assert len(result["errors"]) == 1
        assert result["errors"][0]["project_alias"] == "prod"
        assert result["errors"][0]["error_code"] == "ACCESS_DENIED"
        assert result["errors"][0]["message"] == "Forbidden"

        # Results from dev should still be present
        assert len(result["linked_buckets"]) == 1
        assert result["linked_buckets"][0]["project_alias"] == "dev"

        # Edges from dev still present
        assert len(result["edges"]) == 1

        # Summary reflects the error
        assert result["summary"]["projects_queried"] == 2
        assert result["summary"]["projects_with_errors"] == 1

    def test_client_close_called_even_on_error(self, tmp_config_dir: Path) -> None:
        """Client.close() is called even when list_buckets raises an error."""
        store = _setup_single_project(tmp_config_dir)

        error = KeboolaApiError(
            message="Server Error",
            status_code=500,
            error_code="INTERNAL_ERROR",
        )
        mock_client = _make_failing_lineage_client(error)

        service = LineageService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        result = service.get_lineage(aliases=["prod"])

        # close() should still be called in the finally block
        mock_client.close.assert_called_once()
        assert len(result["errors"]) == 1


class TestLineageNoSharing:
    """Tests for projects with buckets that have no sharing metadata."""

    def test_no_sharing_no_linking(self, tmp_config_dir: Path) -> None:
        """Buckets without sharing, linkedBy, or sourceBucket produce empty results."""
        store = _setup_single_project(tmp_config_dir)

        buckets = [
            {"id": "in.c-data", "name": "data"},
            {"id": "out.c-results", "name": "results"},
        ]
        mock_client = _make_lineage_client(buckets)

        service = LineageService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        result = service.get_lineage(aliases=["prod"])

        assert result["edges"] == []
        assert result["shared_buckets"] == []
        assert result["linked_buckets"] == []
        assert result["summary"]["total_edges"] == 0
        assert result["summary"]["total_shared_buckets"] == 0
        assert result["summary"]["total_linked_buckets"] == 0
        assert result["summary"]["projects_queried"] == 1
        assert result["summary"]["projects_with_errors"] == 0

    def test_empty_bucket_list(self, tmp_config_dir: Path) -> None:
        """A project with no buckets at all produces empty results."""
        store = _setup_single_project(tmp_config_dir)
        mock_client = _make_lineage_client([])

        service = LineageService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        result = service.get_lineage(aliases=["prod"])

        assert result["edges"] == []
        assert result["shared_buckets"] == []
        assert result["linked_buckets"] == []

    def test_empty_linked_by_list(self, tmp_config_dir: Path) -> None:
        """A shared bucket with empty linkedBy list produces no edges."""
        store = _setup_single_project(tmp_config_dir)

        buckets = [
            {
                "id": "in.c-shared",
                "name": "shared",
                "sharing": "organization-project",
                "linkedBy": [],
            },
        ]
        mock_client = _make_lineage_client(buckets)

        service = LineageService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        result = service.get_lineage(aliases=["prod"])

        # Shared bucket is recorded, but no edges
        assert len(result["shared_buckets"]) == 1
        assert result["edges"] == []


class TestLineageNonexistentAlias:
    """Tests for requesting a project alias that does not exist."""

    def test_nonexistent_alias_raises_config_error(self, tmp_config_dir: Path) -> None:
        """Passing an alias not in the config raises ConfigError."""
        store = _setup_single_project(tmp_config_dir, alias="prod")

        service = LineageService(
            config_store=store,
            client_factory=lambda url, token: MagicMock(),
        )

        with pytest.raises(ConfigError, match="Project 'nonexistent' not found"):
            service.get_lineage(aliases=["nonexistent"])

    def test_nonexistent_alias_in_resolve_projects(self, tmp_config_dir: Path) -> None:
        """resolve_projects also raises ConfigError for unknown aliases."""
        store = _setup_single_project(tmp_config_dir, alias="prod")

        service = LineageService(
            config_store=store,
            client_factory=lambda url, token: MagicMock(),
        )

        with pytest.raises(ConfigError, match="Project 'missing' not found"):
            service.resolve_projects(aliases=["missing"])


class TestLineageProjectFilter:
    """Tests for filtering lineage results to specific project aliases."""

    def test_filter_to_single_alias(self, tmp_config_dir: Path) -> None:
        """With 2 projects configured, passing aliases=['prod'] queries only prod."""
        store = _setup_two_projects(tmp_config_dir)

        prod_buckets = [
            {
                "id": "in.c-shared",
                "name": "shared",
                "sharing": "organization-project",
                "linkedBy": [
                    {"project": {"id": 7012, "name": "Development"}, "id": "in.c-linked"},
                ],
            },
        ]
        mock_client = _make_lineage_client(prod_buckets)

        service = LineageService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        result = service.get_lineage(aliases=["prod"])

        # Only 1 project queried
        assert result["summary"]["projects_queried"] == 1

        # The client was called with the prod token
        mock_client.list_buckets.assert_called_once_with(include="linkedBuckets")

        # Shared bucket belongs to prod
        assert len(result["shared_buckets"]) == 1
        assert result["shared_buckets"][0]["project_alias"] == "prod"

    def test_no_aliases_queries_all(self, tmp_config_dir: Path) -> None:
        """Passing aliases=None queries all configured projects."""
        store = _setup_two_projects(tmp_config_dir)

        mock_client = _make_lineage_client([])

        service = LineageService(
            config_store=store,
            client_factory=lambda url, token: mock_client,
        )

        result = service.get_lineage(aliases=None)

        # Both projects should be queried
        assert result["summary"]["projects_queried"] == 2
        assert mock_client.list_buckets.call_count == 2

    def test_resolve_projects_returns_filtered(self, tmp_config_dir: Path) -> None:
        """resolve_projects with aliases returns only the requested project."""
        store = _setup_two_projects(tmp_config_dir)

        service = LineageService(
            config_store=store,
            client_factory=lambda url, token: MagicMock(),
        )

        resolved = service.resolve_projects(aliases=["prod"])
        assert list(resolved.keys()) == ["prod"]
        assert resolved["prod"].project_id == 258

    def test_resolve_projects_no_aliases_returns_all(self, tmp_config_dir: Path) -> None:
        """resolve_projects with aliases=None returns all configured projects."""
        store = _setup_two_projects(tmp_config_dir)

        service = LineageService(
            config_store=store,
            client_factory=lambda url, token: MagicMock(),
        )

        resolved = service.resolve_projects(aliases=None)
        assert set(resolved.keys()) == {"prod", "dev"}
