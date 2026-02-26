"""Lineage service - cross-project data flow analysis via bucket sharing.

Queries the Storage API for bucket sharing metadata and builds
a graph of data flow edges between projects.
"""

from collections.abc import Callable
from typing import Any

from ..client import KeboolaClient
from ..config_store import ConfigStore
from ..errors import ConfigError, KeboolaApiError
from ..models import ProjectConfig

ClientFactory = Callable[[str, str], KeboolaClient]


def default_client_factory(stack_url: str, token: str) -> KeboolaClient:
    """Create a KeboolaClient with the given stack URL and token."""
    return KeboolaClient(stack_url=stack_url, token=token)


class LineageService:
    """Business logic for cross-project data lineage via bucket sharing.

    Queries each project's Storage API for bucket sharing metadata,
    classifies buckets as shared or linked, and builds a deduplicated
    set of data flow edges.

    Uses dependency injection for config_store and client_factory.
    """

    def __init__(
        self,
        config_store: ConfigStore,
        client_factory: ClientFactory | None = None,
    ) -> None:
        self._config_store = config_store
        self._client_factory = client_factory or default_client_factory

    def resolve_projects(self, aliases: list[str] | None = None) -> dict[str, ProjectConfig]:
        """Resolve project aliases to ProjectConfig instances.

        Args:
            aliases: Specific project aliases. If None, returns all.

        Returns:
            Dict mapping alias to ProjectConfig.

        Raises:
            ConfigError: If any specified alias is not found.
        """
        config = self._config_store.load()

        if not aliases:
            return dict(config.projects)

        resolved: dict[str, ProjectConfig] = {}
        for alias in aliases:
            if alias not in config.projects:
                raise ConfigError(f"Project '{alias}' not found.")
            resolved[alias] = config.projects[alias]

        return resolved

    def get_lineage(self, aliases: list[str] | None = None) -> dict[str, Any]:
        """Analyze cross-project data lineage via bucket sharing.

        For each resolved project, fetches buckets with linkedBuckets info,
        classifies them as shared or linked, and builds deduplicated edges.

        Args:
            aliases: Project aliases to query. None means all projects.

        Returns:
            Dict with keys:
                - "edges": list of data flow edge dicts
                - "shared_buckets": list of shared bucket dicts
                - "linked_buckets": list of linked bucket dicts
                - "summary": dict with counts
                - "errors": list of error dicts

        Raises:
            ConfigError: If a specified alias is not found.
        """
        projects = self.resolve_projects(aliases)

        # Build project_id -> alias lookup for cross-referencing
        project_id_to_alias: dict[int, str] = {}
        for alias, project in projects.items():
            project_id_to_alias[project.project_id] = alias

        all_shared_buckets: list[dict[str, Any]] = []
        all_linked_buckets: list[dict[str, Any]] = []
        edges_by_key: dict[tuple[int, str, int, str], dict[str, Any]] = {}
        errors: list[dict[str, str]] = []

        for alias, project in projects.items():
            client = self._client_factory(project.stack_url, project.token)
            try:
                buckets = client.list_buckets(include="linkedBuckets")
                self._process_buckets(
                    buckets=buckets,
                    alias=alias,
                    project=project,
                    project_id_to_alias=project_id_to_alias,
                    all_shared_buckets=all_shared_buckets,
                    all_linked_buckets=all_linked_buckets,
                    edges_by_key=edges_by_key,
                )
            except KeboolaApiError as exc:
                errors.append(
                    {
                        "project_alias": alias,
                        "error_code": exc.error_code,
                        "message": exc.message,
                    }
                )
            finally:
                client.close()

        edges = list(edges_by_key.values())

        summary = {
            "total_shared_buckets": len(all_shared_buckets),
            "total_linked_buckets": len(all_linked_buckets),
            "total_edges": len(edges),
            "projects_queried": len(projects),
            "projects_with_errors": len(errors),
        }

        return {
            "edges": edges,
            "shared_buckets": all_shared_buckets,
            "linked_buckets": all_linked_buckets,
            "summary": summary,
            "errors": errors,
        }

    def _process_buckets(
        self,
        buckets: list[dict[str, Any]],
        alias: str,
        project: ProjectConfig,
        project_id_to_alias: dict[int, str],
        all_shared_buckets: list[dict[str, Any]],
        all_linked_buckets: list[dict[str, Any]],
        edges_by_key: dict[tuple[int, str, int, str], dict[str, Any]],
    ) -> None:
        """Process buckets from a single project, extracting shared/linked info and edges."""
        for bucket in buckets:
            sharing = bucket.get("sharing")
            linked_by = bucket.get("linkedBy", [])
            source_bucket = bucket.get("sourceBucket")

            # Shared bucket: has sharing field
            if sharing:
                shared_info = {
                    "project_alias": alias,
                    "project_id": project.project_id,
                    "project_name": project.project_name,
                    "bucket_id": bucket.get("id", ""),
                    "bucket_name": bucket.get("name", bucket.get("displayName", "")),
                    "sharing_type": sharing,
                    "shared_by": bucket.get("sharedBy", {}),
                }
                all_shared_buckets.append(shared_info)

                # Each linkedBy entry = one edge from this project to target
                for link in linked_by:
                    target_project = link.get("project", {})
                    target_project_id = target_project.get("id", 0)
                    target_bucket_id = link.get("id", "")

                    edge_key = (
                        project.project_id,
                        bucket.get("id", ""),
                        target_project_id,
                        target_bucket_id,
                    )

                    edge = edges_by_key.get(edge_key, {})
                    edge.update(
                        {
                            "source_project_alias": alias,
                            "source_project_id": project.project_id,
                            "source_project_name": project.project_name,
                            "source_bucket_id": bucket.get("id", ""),
                            "source_bucket_name": bucket.get(
                                "name", bucket.get("displayName", "")
                            ),
                            "sharing_type": sharing,
                            "target_project_alias": project_id_to_alias.get(
                                target_project_id, ""
                            ),
                            "target_project_id": target_project_id,
                            "target_project_name": target_project.get("name", ""),
                            "target_bucket_id": target_bucket_id,
                        }
                    )
                    edges_by_key[edge_key] = edge

            # Linked bucket: has sourceBucket field
            if source_bucket:
                source_project = source_bucket.get("project", {})
                linked_info = {
                    "project_alias": alias,
                    "project_id": project.project_id,
                    "project_name": project.project_name,
                    "bucket_id": bucket.get("id", ""),
                    "bucket_name": bucket.get("name", bucket.get("displayName", "")),
                    "source_bucket_id": source_bucket.get("id", ""),
                    "source_project_id": source_project.get("id", 0),
                    "source_project_name": source_project.get("name", ""),
                    "is_readonly": bucket.get("isReadonly", False),
                }
                all_linked_buckets.append(linked_info)

                source_project_id = source_project.get("id", 0)
                source_bucket_id = source_bucket.get("id", "")

                edge_key = (
                    source_project_id,
                    source_bucket_id,
                    project.project_id,
                    bucket.get("id", ""),
                )

                edge = edges_by_key.get(edge_key, {})
                # Only set fields that are empty or missing
                if not edge.get("source_project_alias"):
                    edge["source_project_alias"] = project_id_to_alias.get(
                        source_project_id, ""
                    )
                if not edge.get("source_project_id"):
                    edge["source_project_id"] = source_project_id
                if not edge.get("source_project_name"):
                    edge["source_project_name"] = source_project.get("name", "")
                if not edge.get("source_bucket_id"):
                    edge["source_bucket_id"] = source_bucket_id
                # Always set target side from linked bucket
                edge["target_project_alias"] = alias
                edge["target_project_id"] = project.project_id
                edge["target_project_name"] = project.project_name
                edge["target_bucket_id"] = bucket.get("id", "")
                # Preserve sharing_type if already set from shared side
                if not edge.get("sharing_type"):
                    edge["sharing_type"] = ""

                edges_by_key[edge_key] = edge
