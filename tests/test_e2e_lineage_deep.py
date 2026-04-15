"""End-to-end tests for `kbagent lineage deep` against real sync'd data.

Requires a pre-configured workspace directory with sync'd projects.
Set E2E_LINEAGE_DIR to the path (e.g. /tmp/lineage-ro).

Run:
    E2E_LINEAGE_DIR=/tmp/lineage-ro uv run pytest tests/test_e2e_lineage_deep.py -v -s
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from keboola_agent_cli.cli import app
from keboola_agent_cli.config_store import ConfigStore

# ---------------------------------------------------------------------------
# Environment & skip logic
# ---------------------------------------------------------------------------

ENV_LINEAGE_DIR = "E2E_LINEAGE_DIR"

LINEAGE_DIR = os.environ.get(ENV_LINEAGE_DIR, "")
HAS_LINEAGE_DIR = bool(LINEAGE_DIR) and Path(LINEAGE_DIR).is_dir()

skip_without_lineage_dir = pytest.mark.skipif(
    not HAS_LINEAGE_DIR,
    reason=f"Lineage deep E2E tests require {ENV_LINEAGE_DIR} env var pointing to sync'd workspace",
)

runner = CliRunner()

# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

_DIM = "\033[2m"
_CYAN = "\033[36m"
_GREEN = "\033[32m"
_RED = "\033[31m"
_BOLD = "\033[1m"
_RESET = "\033[0m"
_MAX_RESPONSE_LEN = 500


def _format_cmd(args: list[str]) -> str:
    return "kbagent " + " ".join(args)


def _summarize(output: str, max_len: int = _MAX_RESPONSE_LEN) -> str:
    try:
        data = json.loads(output)
        pretty = json.dumps(data, indent=2, ensure_ascii=False)
        if len(pretty) > max_len:
            return pretty[:max_len] + f"\n  ... ({len(pretty)} chars total)"
        return pretty
    except (json.JSONDecodeError, TypeError):
        text = output.strip()
        return text[:max_len] + "..." if len(text) > max_len else text


def _invoke(config_dir: Path, args: list[str]) -> Any:
    """Invoke the CLI with a custom config store."""
    print(f"\n  {_CYAN}$ {_format_cmd(args)}{_RESET}")

    with patch("keboola_agent_cli.cli.ConfigStore") as mock_cls:
        mock_cls.return_value = ConfigStore(config_dir=config_dir)
        result = runner.invoke(app, args, catch_exceptions=True)

    status = (
        f"{_GREEN}OK{_RESET}" if result.exit_code == 0 else f"{_RED}EXIT {result.exit_code}{_RESET}"
    )
    print(f"  {_DIM}-> {status} ({len(result.output)} bytes){_RESET}")
    for line in _summarize(result.output).split("\n")[:10]:
        print(f"  {_DIM}   {line}{_RESET}")

    return result


def _json_ok(result) -> dict[str, Any]:
    """Parse CLI output as JSON and assert status == ok."""
    assert result.exit_code == 0, (
        f"Command failed (exit {result.exit_code}):\n{result.output[:500]}"
    )
    data = json.loads(result.output)
    assert data.get("status") == "ok", f"Expected status=ok, got: {json.dumps(data)[:300]}"
    return data


def _step(num: int, title: str) -> None:
    print(f"\n{_BOLD}{'=' * 60}")
    print(f"  STEP {num}: {title}")
    print(f"{'=' * 60}{_RESET}")


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


@skip_without_lineage_dir
@pytest.mark.e2e
class TestE2ELineageDeep:
    """E2E tests for lineage deep against real sync'd data."""

    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        self.lineage_dir = Path(LINEAGE_DIR).resolve()
        self.config_dir = self.lineage_dir / ".kbagent"
        self.cache_file = self.lineage_dir / "lineage.json"

    # ── Build ──────────────────────────────────────────────────────

    def test_01_build_lineage(self) -> None:
        """Build lineage graph from sync'd data and save cache."""
        _step(1, "Build lineage from sync'd data")

        result = _invoke(
            self.config_dir,
            [
                "--json",
                "lineage",
                "deep",
                "-d",
                str(self.lineage_dir),
                "-o",
                str(self.cache_file),
            ],
        )
        data = _json_ok(result)

        summary = data["data"]["summary"]
        assert summary["tables"] > 0, "Expected tables in lineage"
        assert summary["configurations"] > 0, "Expected configs in lineage"
        assert summary["edges"] > 0, "Expected edges in lineage"

        # Verify detection methods
        methods = summary["detection_methods"]
        assert "input_mapping" in methods, "Missing input_mapping edges"
        assert "output_mapping" in methods, "Missing output_mapping edges"
        assert (
            methods.get("sql_tokenizer", 0) + methods.get("sql_tokenizer_cross_project", 0) > 0
        ), "SQL tokenizer should find at least some edges"

        print(
            f"\n  {_GREEN}Summary: {summary['tables']} tables, "
            f"{summary['configurations']} configs, {summary['edges']} edges{_RESET}"
        )

    # ── Load from cache ────────────────────────────────────────────

    def test_02_load_cache_summary(self) -> None:
        """Load from cache and show summary (human mode)."""
        _step(2, "Load from cache - summary")

        result = _invoke(
            self.config_dir,
            [
                "lineage",
                "deep",
                "-l",
                str(self.cache_file),
            ],
        )
        assert result.exit_code == 0
        assert "Lineage Graph Summary" in result.output
        assert "Tables:" in result.output
        assert "Edges:" in result.output

    # ── Downstream query ───────────────────────────────────────────

    def test_03_downstream_json(self) -> None:
        """Query downstream in JSON mode - SFDC company table."""
        _step(3, "Downstream query (JSON)")

        result = _invoke(
            self.config_dir,
            [
                "--json",
                "lineage",
                "deep",
                "-l",
                str(self.cache_file),
                "--downstream",
                "ir-l0-sales-marketing:out.c-sfdc.company",
                "--depth",
                "2",
            ],
        )
        data = _json_ok(result)
        edges = data["data"]["edges"]
        assert len(edges) > 0, "SFDC company should have downstream consumers"

        # Should have cross-project SQL references
        detections = {e["detection"] for e in edges}
        assert "sql_tokenizer_cross_project" in detections, (
            "Expected cross-project SQL refs for SFDC company"
        )

        print(f"\n  {_GREEN}Found {len(edges)} downstream edges{_RESET}")

    def test_04_downstream_human(self) -> None:
        """Query downstream in human mode."""
        _step(4, "Downstream query (human)")

        result = _invoke(
            self.config_dir,
            [
                "lineage",
                "deep",
                "-l",
                str(self.cache_file),
                "--downstream",
                "ir-l0-sales-marketing:out.c-sfdc.company",
                "--depth",
                "1",
            ],
        )
        assert result.exit_code == 0
        assert "Downstream dependents" in result.output

    # ── Upstream query ─────────────────────────────────────────────

    def test_05_upstream_json(self) -> None:
        """Query upstream in JSON mode - Vertex AI output table."""
        _step(5, "Upstream query (JSON)")

        result = _invoke(
            self.config_dir,
            [
                "--json",
                "lineage",
                "deep",
                "-l",
                str(self.cache_file),
                "--upstream",
                "engg-cloud-costs:out.c-Vertex-AI-Usage---Last-7-Days.vertex_ai_weekly_comparison",
            ],
        )
        data = _json_ok(result)
        edges = data["data"]["edges"]
        assert len(edges) >= 2, "Vertex AI table should have transformation + source table upstream"

        # Verify node_info
        node_info = data["data"]["node_info"]
        assert node_info["type"] == "table"
        assert node_info["columns"] > 0

        print(
            f"\n  {_GREEN}Found {len(edges)} upstream edges, "
            f"table has {node_info['columns']} cols{_RESET}"
        )

    # ── Column detail (--columns) ──────────────────────────────────

    def test_06_columns_flag(self) -> None:
        """--columns shows AI-detected column mappings."""
        _step(6, "Column detail (--columns flag)")

        result = _invoke(
            self.config_dir,
            [
                "lineage",
                "deep",
                "-l",
                str(self.cache_file),
                "--upstream",
                "kids-app-factory:out.c-cs_app_upsell.company_metrics",
                "--depth",
                "2",
                "--columns",
            ],
        )
        assert result.exit_code == 0
        # Should contain column mapping lines like "company_id <- ..."
        assert "<-" in result.output, "Expected column mapping arrows in output"

    # ── Single column trace (-c) ───────────────────────────────────

    def test_07_column_trace(self) -> None:
        """Trace a specific column through lineage."""
        _step(7, "Column trace (-c company_id)")

        result = _invoke(
            self.config_dir,
            [
                "lineage",
                "deep",
                "-l",
                str(self.cache_file),
                "--upstream",
                "kids-app-factory:out.c-cs_app_upsell.company_metrics",
                "--depth",
                "2",
                "--columns",
                "-c",
                "company_id",
            ],
        )
        assert result.exit_code == 0
        assert "column: company_id" in result.output
        # Should show company_id mapping
        assert "company_id" in result.output

    # ── Node not found ─────────────────────────────────────────────

    def test_08_node_not_found_json(self) -> None:
        """Missing node returns structured error."""
        _step(8, "Node not found (JSON)")

        result = _invoke(
            self.config_dir,
            [
                "--json",
                "lineage",
                "deep",
                "-l",
                str(self.cache_file),
                "--upstream",
                "nonexistent:fake.table",
            ],
        )
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["status"] == "error"
        assert "NODE_NOT_FOUND" in data["error"]["code"]

    def test_09_node_not_found_human(self) -> None:
        """Missing node shows error in human mode."""
        _step(9, "Node not found (human)")

        result = _invoke(
            self.config_dir,
            [
                "lineage",
                "deep",
                "-l",
                str(self.cache_file),
                "--downstream",
                "nonexistent.table.xyz",
            ],
        )
        assert result.exit_code == 1

    # ── Missing cache file ─────────────────────────────────────────

    def test_10_missing_cache(self) -> None:
        """Non-existent cache file returns error."""
        _step(10, "Missing cache file")

        result = _invoke(
            self.config_dir,
            [
                "--json",
                "lineage",
                "deep",
                "-l",
                "/tmp/does_not_exist_lineage.json",
            ],
        )
        assert result.exit_code == 1

    # ── --hint service ─────────────────────────────────────────────

    def test_11_hint_service(self) -> None:
        """--hint service generates Python code."""
        _step(11, "--hint service generates Python")

        result = _invoke(
            self.config_dir,
            [
                "--hint",
                "service",
                "lineage",
                "deep",
                "-d",
                str(self.lineage_dir),
                "--upstream",
                "engg-cloud-costs:out.c-Vertex-AI-Usage---Last-7-Days.vertex_ai_weekly_comparison",
            ],
        )
        assert result.exit_code == 0
        assert "DeepLineageService" in result.output
        assert "build_lineage" in result.output
        assert "query_upstream" in result.output

    # ── FQN resolution ─────────────────────────────────────────────

    def test_12_fqn_with_project(self) -> None:
        """Full FQN with project:table_id resolves correctly."""
        _step(12, "FQN with project prefix")

        result = _invoke(
            self.config_dir,
            [
                "--json",
                "lineage",
                "deep",
                "-l",
                str(self.cache_file),
                "--downstream",
                "keboola-ai:out.c-mcp-analysis.mcp_usage_analysis",
                "--depth",
                "1",
            ],
        )
        data = _json_ok(result)
        assert data["data"]["node"] == "keboola-ai:out.c-mcp-analysis.mcp_usage_analysis"
        assert len(data["data"]["edges"]) > 0

    # ── Depth control ──────────────────────────────────────────────

    def test_13_depth_limits_traversal(self) -> None:
        """--depth 1 should return fewer edges than --depth 5."""
        _step(13, "Depth control")

        result_shallow = _invoke(
            self.config_dir,
            [
                "--json",
                "lineage",
                "deep",
                "-l",
                str(self.cache_file),
                "--downstream",
                "ir-l0-sales-marketing:out.c-sfdc.company",
                "--depth",
                "1",
            ],
        )
        result_deep = _invoke(
            self.config_dir,
            [
                "--json",
                "lineage",
                "deep",
                "-l",
                str(self.cache_file),
                "--downstream",
                "ir-l0-sales-marketing:out.c-sfdc.company",
                "--depth",
                "5",
            ],
        )
        shallow = _json_ok(result_shallow)
        deep = _json_ok(result_deep)

        shallow_count = len(shallow["data"]["edges"])
        deep_count = len(deep["data"]["edges"])
        assert deep_count >= shallow_count, (
            f"depth=5 ({deep_count}) should find >= edges than depth=1 ({shallow_count})"
        )

        print(f"\n  {_GREEN}depth=1: {shallow_count} edges, depth=5: {deep_count} edges{_RESET}")

    # ── Permission check ───────────────────────────────────────────

    def test_14_permission_registered(self) -> None:
        """lineage.deep is registered in the permission system."""
        _step(14, "Permission registration check")

        result = _invoke(
            self.config_dir,
            [
                "--json",
                "lineage",
                "deep",
                "-l",
                str(self.cache_file),
            ],
        )
        # If permission was missing, we'd get exit code 6
        assert result.exit_code == 0
