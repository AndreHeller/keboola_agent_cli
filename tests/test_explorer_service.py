"""Tests for ExplorerService and explorer helper functions."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from helpers import setup_single_project
from keboola_agent_cli.services.explorer_service import (
    ExplorerService,
    _assign_tier,
    _build_mermaid,
    _compute_job_stats,
    _default_output_dir,
    _type_icon,
)

# ---------------------------------------------------------------------------
# Pure function tests: _assign_tier
# ---------------------------------------------------------------------------

class TestAssignTier:

    def test_l0_convention(self) -> None:
        tier, unclassified = _assign_tier("acme-l0-extract")
        assert tier == "L0"
        assert not unclassified

    def test_l0_prefix_convention(self) -> None:
        tier, unclassified = _assign_tier("l0-extract")
        assert tier == "L0"
        assert not unclassified

    def test_l1_convention(self) -> None:
        tier, unclassified = _assign_tier("acme-l1-transform")
        assert tier == "L1"
        assert not unclassified

    def test_l1_prefix_convention(self) -> None:
        tier, unclassified = _assign_tier("l1-transform")
        assert tier == "L1"
        assert not unclassified

    def test_l2_convention(self) -> None:
        tier, unclassified = _assign_tier("acme-l2-delivery")
        assert tier == "L2"
        assert not unclassified

    def test_l2_prefix_convention(self) -> None:
        tier, unclassified = _assign_tier("l2-delivery")
        assert tier == "L2"
        assert not unclassified

    def test_default_unclassified(self) -> None:
        tier, unclassified = _assign_tier("my-project")
        assert tier == "L0"
        assert unclassified

    def test_tier_map_override(self) -> None:
        tier, unclassified = _assign_tier("my-project", {"my-project": "L2"})
        assert tier == "L2"
        assert not unclassified

    def test_tier_map_takes_precedence_over_convention(self) -> None:
        tier, unclassified = _assign_tier("acme-l1-transform", {"acme-l1-transform": "L0"})
        assert tier == "L0"
        assert not unclassified


# ---------------------------------------------------------------------------
# Pure function tests: _compute_job_stats
# ---------------------------------------------------------------------------

class TestComputeJobStats:

    def test_empty(self) -> None:
        stats = _compute_job_stats([])
        assert stats["total_jobs"] == 0
        assert stats["status_counts"] == {}
        assert stats["success_rate_pct"] == 0
        assert stats["avg_duration_seconds"] == 0
        assert stats["date_range"]["earliest"] is None
        assert stats["failing_configs"] == []

    def test_mixed(self) -> None:
        jobs = [
            {"status": "success", "durationSeconds": 10, "createdTime": "2025-01-01T00:00:00Z",
             "component": "keboola.ex-db", "configId": "1"},
            {"status": "success", "durationSeconds": 20, "createdTime": "2025-01-02T00:00:00Z",
             "component": "keboola.ex-db", "configId": "1"},
            {"status": "error", "durationSeconds": 5, "createdTime": "2025-01-03T00:00:00Z",
             "component": "keboola.wr-db", "configId": "2"},
        ]
        stats = _compute_job_stats(jobs)
        assert stats["total_jobs"] == 3
        assert stats["status_counts"]["success"] == 2
        assert stats["status_counts"]["error"] == 1
        assert stats["success_rate_pct"] == pytest.approx(66.7, abs=0.1)
        assert stats["avg_duration_seconds"] == pytest.approx(11.7, abs=0.1)
        assert stats["date_range"]["earliest"] == "2025-01-01T00:00:00Z"
        assert stats["date_range"]["latest"] == "2025-01-03T00:00:00Z"

    def test_failing_configs_sorted(self) -> None:
        jobs = [
            {"status": "error", "component": "c1", "configId": "a",
             "createdTime": "2025-01-01T00:00:00Z"},
            {"status": "success", "component": "c1", "configId": "a",
             "createdTime": "2025-01-02T00:00:00Z"},
            {"status": "error", "component": "c2", "configId": "b",
             "createdTime": "2025-01-01T00:00:00Z"},
        ]
        stats = _compute_job_stats(jobs)
        assert len(stats["failing_configs"]) == 2
        # c2/b has 100% error rate, c1/a has 50%
        assert stats["failing_configs"][0]["config_key"] == "c2/b"
        assert stats["failing_configs"][0]["error_rate_pct"] == 100.0
        assert stats["failing_configs"][1]["config_key"] == "c1/a"
        assert stats["failing_configs"][1]["error_rate_pct"] == 50.0


# ---------------------------------------------------------------------------
# Pure function tests: _type_icon
# ---------------------------------------------------------------------------

class TestTypeIcon:

    def test_extractor(self) -> None:
        assert _type_icon("keboola.ex-google-drive") == "EX"

    def test_writer(self) -> None:
        assert _type_icon("keboola.wr-snowflake") == "WR"

    def test_transformation(self) -> None:
        assert _type_icon("keboola.snowflake-transformation") == "TR"

    def test_snowflake_sql(self) -> None:
        assert _type_icon("keboola.snowflake-sql") == "TR"

    def test_orchestrator(self) -> None:
        assert _type_icon("keboola.orchestrator") == "OT"

    def test_application(self) -> None:
        assert _type_icon("keboola.app-something") == "AP"


# ---------------------------------------------------------------------------
# Pure function tests: _default_output_dir
# ---------------------------------------------------------------------------

class TestDefaultOutputDir:

    def test_returns_cwd_based_path(self) -> None:
        result = _default_output_dir()
        assert result == Path.cwd() / "kbc-explorer"

    def test_returns_path_instance(self) -> None:
        result = _default_output_dir()
        assert isinstance(result, Path)


# ---------------------------------------------------------------------------
# Pure function tests: _build_mermaid
# ---------------------------------------------------------------------------

class TestBuildMermaid:

    def test_basic_graph(self) -> None:
        phases = [
            {
                "id": 1,
                "name": "Extract",
                "depends_on": [],
                "tasks": [
                    {"name": "Pull data", "config_id": "100", "type_icon": "EX"},
                ],
            },
            {
                "id": 2,
                "name": "Transform",
                "depends_on": [1],
                "tasks": [],
            },
        ]
        result = _build_mermaid(phases)
        assert result.startswith("graph TD")
        assert 'P1["Extract"]' in result
        assert 'P2["Transform"]' in result
        assert "P1 --> P2" in result
        assert 'P1_100["EX Pull data"]' in result


# ---------------------------------------------------------------------------
# _parse_orchestration tests (real API structure)
# ---------------------------------------------------------------------------

class TestParseOrchestration:
    """Tests for _parse_orchestration with realistic Keboola Flow API responses.

    The API returns phases and tasks as separate arrays:
    - configuration.phases: [{id, name, dependsOn: [int, ...]}]
    - configuration.tasks: [{id, name, phase: <phase_id>, task: {componentId, configId}, ...}]
    """

    def test_phases_and_tasks_linked_by_phase_id(self) -> None:
        cfg = {"config_name": "My Flow", "config_description": "Desc"}
        detail = {
            "configuration": {
                "parameters": {},
                "phases": [
                    {"id": 100, "name": "Extract", "dependsOn": []},
                    {"id": 200, "name": "Transform", "dependsOn": [100]},
                ],
                "tasks": [
                    {
                        "id": 1, "name": "pull-data", "phase": 100,
                        "task": {"componentId": "keboola.ex-google-drive", "configId": "abc"},
                        "enabled": True, "continueOnFailure": False,
                    },
                    {
                        "id": 2, "name": "snowflake-sql", "phase": 200,
                        "task": {"componentId": "keboola.snowflake-transformation", "configId": "def"},
                        "enabled": True, "continueOnFailure": True,
                    },
                ],
            },
            "name": "My Flow",
            "description": "Desc",
            "isDisabled": False,
            "version": 5,
            "changeDescription": "Updated",
        }
        result = ExplorerService._parse_orchestration("proj", "cfg1", cfg, detail)

        assert result["total_phases"] == 2
        assert result["total_tasks"] == 2
        assert result["phases"][0]["name"] == "Extract"
        assert result["phases"][0]["depends_on"] == []
        assert len(result["phases"][0]["tasks"]) == 1
        assert result["phases"][0]["tasks"][0]["component_id"] == "keboola.ex-google-drive"
        assert result["phases"][0]["tasks"][0]["type_icon"] == "EX"

        assert result["phases"][1]["name"] == "Transform"
        assert result["phases"][1]["depends_on"] == [100]
        assert len(result["phases"][1]["tasks"]) == 1
        assert result["phases"][1]["tasks"][0]["type_icon"] == "TR"
        assert result["phases"][1]["tasks"][0]["continue_on_failure"] is True

    def test_depends_on_as_raw_integers(self) -> None:
        """dependsOn from the API is a list of integers, not objects."""
        cfg = {"config_name": "Flow", "config_description": ""}
        detail = {
            "configuration": {
                "phases": [
                    {"id": 1, "name": "A", "dependsOn": []},
                    {"id": 2, "name": "B", "dependsOn": [1]},
                    {"id": 3, "name": "C", "dependsOn": [1, 2]},
                ],
                "tasks": [],
            },
            "name": "Flow", "description": "", "isDisabled": False, "version": 1,
        }
        result = ExplorerService._parse_orchestration("proj", "cfg1", cfg, detail)
        assert result["phases"][0]["depends_on"] == []
        assert result["phases"][1]["depends_on"] == [1]
        assert result["phases"][2]["depends_on"] == [1, 2]

    def test_depends_on_as_dict_objects(self) -> None:
        """Handle legacy format where dependsOn contains {phaseId: N} objects."""
        cfg = {"config_name": "Flow", "config_description": ""}
        detail = {
            "configuration": {
                "phases": [
                    {"id": 1, "name": "A", "dependsOn": []},
                    {"id": 2, "name": "B", "dependsOn": [{"phaseId": 1}]},
                ],
                "tasks": [],
            },
            "name": "Flow", "description": "", "isDisabled": False, "version": 1,
        }
        result = ExplorerService._parse_orchestration("proj", "cfg1", cfg, detail)
        assert result["phases"][1]["depends_on"] == [1]

    def test_empty_orchestration(self) -> None:
        cfg = {"config_name": "Empty", "config_description": ""}
        detail = {
            "configuration": {"phases": [], "tasks": []},
            "name": "Empty", "description": "", "isDisabled": False, "version": 1,
        }
        result = ExplorerService._parse_orchestration("proj", "cfg1", cfg, detail)
        assert result["total_phases"] == 0
        assert result["total_tasks"] == 0
        assert result["phases"] == []
        assert result["mermaid"] == "graph TD"

    def test_task_with_non_dict_task_field(self) -> None:
        """Guard against task.task being a string instead of a dict."""
        cfg = {"config_name": "Flow", "config_description": ""}
        detail = {
            "configuration": {
                "phases": [{"id": 1, "name": "A", "dependsOn": []}],
                "tasks": [
                    {"id": 1, "name": "odd-task", "phase": 1, "task": "some-string",
                     "enabled": True, "continueOnFailure": False},
                ],
            },
            "name": "Flow", "description": "", "isDisabled": False, "version": 1,
        }
        result = ExplorerService._parse_orchestration("proj", "cfg1", cfg, detail)
        assert result["total_tasks"] == 1
        assert result["phases"][0]["tasks"][0]["component_id"] == ""


# ---------------------------------------------------------------------------
# Service tests with mocked dependencies
# ---------------------------------------------------------------------------

def _make_mock_services(alias: str = "prod"):
    """Create mocked ConfigService, JobService, LineageService."""
    config_svc = MagicMock()
    config_svc.list_configs.return_value = {
        "configs": [
            {
                "project_alias": alias,
                "config_id": "1",
                "config_name": "My Extractor",
                "config_description": "",
                "component_id": "keboola.ex-db",
                "component_name": "DB Extractor",
                "component_type": "extractor",
            },
        ],
        "errors": [],
    }
    config_svc.get_config_detail.return_value = {
        "configuration": {"phases": [], "tasks": []},
        "name": "My Extractor",
        "description": "",
        "isDisabled": False,
        "version": 1,
    }

    job_svc = MagicMock()
    job_svc.list_jobs.return_value = {
        "jobs": [
            {
                "project_alias": alias,
                "status": "success",
                "durationSeconds": 10,
                "createdTime": "2025-01-01T00:00:00Z",
                "component": "keboola.ex-db",
                "configId": "1",
            },
        ],
        "errors": [],
    }

    lineage_svc = MagicMock()
    lineage_svc.get_lineage.return_value = {"edges": [], "errors": []}

    return config_svc, job_svc, lineage_svc


class TestExplorerServiceGenerate:

    def test_generate_single_project(self, tmp_path: Path) -> None:
        store = setup_single_project(tmp_path / "config", alias="l0-prod")
        config_svc, job_svc, lineage_svc = _make_mock_services("l0-prod")
        output_dir = tmp_path / "output"

        service = ExplorerService(
            config_store=store,
            config_service=config_svc,
            job_service=job_svc,
            lineage_service=lineage_svc,
        )
        result = service.generate(output_dir=output_dir, open_browser=False)

        assert result["projects_count"] == 1
        assert result["configs_count"] == 1
        assert result["jobs_sampled"] == 1
        assert len(result["files_written"]) == 2

        # Verify catalog structure
        catalog = json.loads((output_dir / "catalog.json").read_text())
        assert "metadata" in catalog
        assert "tiers" in catalog
        assert "projects" in catalog
        assert "lineage" in catalog
        assert "orchestrations" in catalog
        assert "l0-prod" in catalog["projects"]
        assert catalog["projects"]["l0-prod"]["tier"] == "L0"

    def test_generate_writes_four_files(self, tmp_path: Path) -> None:
        store = setup_single_project(tmp_path / "config", alias="l0-prod")
        config_svc, job_svc, lineage_svc = _make_mock_services("l0-prod")
        output_dir = tmp_path / "output"

        service = ExplorerService(
            config_store=store,
            config_service=config_svc,
            job_service=job_svc,
            lineage_service=lineage_svc,
        )
        service.generate(output_dir=output_dir, open_browser=False)

        assert (output_dir / "catalog.json").exists()
        assert (output_dir / "catalog.js").exists()

        # JS file wraps JSON in a variable assignment
        js_content = (output_dir / "catalog.js").read_text()
        assert js_content.startswith("const CATALOG = ")

    def test_generate_atomic_writes_no_leftover_tmp(self, tmp_path: Path) -> None:
        store = setup_single_project(tmp_path / "config", alias="l0-prod")
        config_svc, job_svc, lineage_svc = _make_mock_services("l0-prod")
        output_dir = tmp_path / "output"

        service = ExplorerService(
            config_store=store,
            config_service=config_svc,
            job_service=job_svc,
            lineage_service=lineage_svc,
        )
        service.generate(output_dir=output_dir, open_browser=False)

        # No .tmp files should remain after successful generation
        tmp_files = list(output_dir.glob("*.tmp"))
        assert tmp_files == []

    def test_generate_tier_config_override(self, tmp_path: Path) -> None:
        store = setup_single_project(tmp_path / "config", alias="my-project")
        config_svc, job_svc, lineage_svc = _make_mock_services("my-project")

        output_dir = tmp_path / "output"
        tiers_file = tmp_path / "tiers.yaml"
        tiers_file.write_text(
            "description: My ecosystem\n"
            "tiers:\n"
            "  L0:\n"
            "    name: Sources\n"
            "    description: Data sources\n"
            "projects:\n"
            "  my-project: L2\n"
        )

        service = ExplorerService(
            config_store=store,
            config_service=config_svc,
            job_service=job_svc,
            lineage_service=lineage_svc,
        )
        result = service.generate(
            output_dir=output_dir, open_browser=False, tiers_config=tiers_file,
        )

        catalog = json.loads((output_dir / "catalog.json").read_text())
        assert catalog["projects"]["my-project"]["tier"] == "L2"
        assert catalog["metadata"]["description"] == "My ecosystem"
        # No TIER_UNCLASSIFIED warnings
        tier_warnings = [e for e in result["errors"] if e["error_code"] == "TIER_UNCLASSIFIED"]
        assert tier_warnings == []

    def test_generate_unclassified_warning(self, tmp_path: Path) -> None:
        store = setup_single_project(tmp_path / "config", alias="my-project")
        config_svc, job_svc, lineage_svc = _make_mock_services("my-project")

        output_dir = tmp_path / "output"

        service = ExplorerService(
            config_store=store,
            config_service=config_svc,
            job_service=job_svc,
            lineage_service=lineage_svc,
        )
        result = service.generate(output_dir=output_dir, open_browser=False)

        tier_warnings = [e for e in result["errors"] if e["error_code"] == "TIER_UNCLASSIFIED"]
        assert len(tier_warnings) == 1
        assert "my-project" in tier_warnings[0]["message"]
        assert "defaulting to L0" in tier_warnings[0]["message"]

    def test_generate_schema_validation(self, tmp_path: Path) -> None:
        store = setup_single_project(tmp_path / "config", alias="l0-prod")
        config_svc, job_svc, lineage_svc = _make_mock_services("l0-prod")
        output_dir = tmp_path / "output"
        output_dir.mkdir(parents=True)

        # Copy schema into output dir
        schema_src = Path(__file__).resolve().parent.parent / "kbc-explorer" / "schema.json"
        if schema_src.exists():
            (output_dir / "schema.json").write_text(schema_src.read_text())

        service = ExplorerService(
            config_store=store,
            config_service=config_svc,
            job_service=job_svc,
            lineage_service=lineage_svc,
        )
        result = service.generate(output_dir=output_dir, open_browser=False)

        # Should pass validation (no SCHEMA_VALIDATION_ERROR)
        schema_errors = [
            e for e in result["errors"] if e["error_code"] == "SCHEMA_VALIDATION_ERROR"
        ]
        assert schema_errors == []

    def test_generate_schema_validation_catches_invalid(self, tmp_path: Path) -> None:
        store = setup_single_project(tmp_path / "config", alias="l0-prod")
        config_svc, job_svc, lineage_svc = _make_mock_services("l0-prod")
        output_dir = tmp_path / "output"
        output_dir.mkdir(parents=True)

        # Write a strict schema that will reject the catalog
        strict_schema = {
            "type": "object",
            "required": ["metadata", "tiers", "projects", "lineage", "nonexistent_field"],
        }
        (output_dir / "schema.json").write_text(json.dumps(strict_schema))

        service = ExplorerService(
            config_store=store,
            config_service=config_svc,
            job_service=job_svc,
            lineage_service=lineage_svc,
        )
        result = service.generate(output_dir=output_dir, open_browser=False)

        schema_errors = [
            e for e in result["errors"] if e["error_code"] == "SCHEMA_VALIDATION_ERROR"
        ]
        assert len(schema_errors) == 1
        assert "nonexistent_field" in schema_errors[0]["message"]

        # Files should still be written despite validation failure
        assert (output_dir / "catalog.json").exists()
