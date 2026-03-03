"""Explorer service - generates catalog and orchestration data for kbc-explorer.

Orchestrates data collection across all registered projects by calling
ConfigService, JobService, and LineageService, then assembles the results
into catalog.json/catalog.js and orchestrations.json/orchestrations.js
files that the kbc-explorer HTML app consumes.
"""

import json
import logging
import os
import webbrowser
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import jsonschema
import yaml

from .. import __version__
from ..config_store import ConfigStore
from ..constants import MAX_JOB_LIMIT
from ..errors import ConfigError
from .base import BaseService, ClientFactory
from .config_service import ConfigService
from .job_service import JobService
from .lineage_service import LineageService

logger = logging.getLogger(__name__)


def _default_output_dir() -> Path:
    """Return the default output directory for explorer files (relative to CWD)."""
    return Path.cwd() / "kbc-explorer"


def _assign_tier(alias: str, tier_map: dict[str, str] | None = None) -> tuple[str, bool]:
    """Assign a tier based on tier config, then alias naming convention.

    Returns:
        Tuple of (tier, was_unclassified). was_unclassified is True when
        neither the tier map nor the naming convention matched.
    """
    if tier_map and alias in tier_map:
        return tier_map[alias], False

    lower = alias.lower()
    if "-l0-" in lower or lower.startswith("l0-"):
        return "L0", False
    if "-l1-" in lower or lower.startswith("l1-"):
        return "L1", False
    if "-l2-" in lower or lower.startswith("l2-"):
        return "L2", False
    return "L0", True


def _compute_job_stats(jobs: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute aggregated job statistics from a list of raw job dicts."""
    total = len(jobs)
    if total == 0:
        return {
            "total_jobs": 0,
            "status_counts": {},
            "success_rate_pct": 0,
            "avg_duration_seconds": 0,
            "date_range": {"earliest": None, "latest": None},
            "component_stats": {},
            "failing_configs": [],
        }

    status_counts: dict[str, int] = {}
    durations: list[float] = []
    timestamps: list[str] = []
    component_stats: dict[str, dict[str, int]] = {}
    config_stats: dict[str, dict[str, Any]] = {}

    for job in jobs:
        status = job.get("status", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

        duration = job.get("durationSeconds")
        if duration is not None:
            durations.append(float(duration))

        created = job.get("createdTime") or job.get("startTime")
        if created:
            timestamps.append(created)

        comp_id = job.get("component", job.get("componentId", "unknown"))
        if comp_id not in component_stats:
            component_stats[comp_id] = {"success": 0, "error": 0, "other": 0, "total": 0}
        component_stats[comp_id]["total"] += 1
        if status == "success":
            component_stats[comp_id]["success"] += 1
        elif status == "error":
            component_stats[comp_id]["error"] += 1
        else:
            component_stats[comp_id]["other"] += 1

        config_val = job.get("configId") or job.get("config", "")
        config_id = str(config_val) if config_val else ""
        if config_id:
            config_key = f"{comp_id}/{config_id}"
            if config_key not in config_stats:
                config_stats[config_key] = {
                    "config_key": config_key,
                    "component_id": comp_id,
                    "error_count": 0,
                    "total_runs": 0,
                    "last_run": "",
                }
            config_stats[config_key]["total_runs"] += 1
            if status == "error":
                config_stats[config_key]["error_count"] += 1
            if created and created > config_stats[config_key]["last_run"]:
                config_stats[config_key]["last_run"] = created

    success_count = status_counts.get("success", 0)
    success_rate = round((success_count / total) * 100, 1) if total > 0 else 0
    avg_duration = round(sum(durations) / len(durations), 1) if durations else 0

    timestamps.sort()
    date_range = {
        "earliest": timestamps[0] if timestamps else None,
        "latest": timestamps[-1] if timestamps else None,
    }

    failing_configs = []
    for cs in config_stats.values():
        if cs["error_count"] > 0:
            cs["error_rate_pct"] = round(
                (cs["error_count"] / cs["total_runs"]) * 100, 1
            )
            failing_configs.append(cs)
    failing_configs.sort(key=lambda x: x["error_rate_pct"], reverse=True)

    return {
        "total_jobs": total,
        "status_counts": status_counts,
        "success_rate_pct": success_rate,
        "avg_duration_seconds": avg_duration,
        "date_range": date_range,
        "component_stats": component_stats,
        "failing_configs": failing_configs,
    }


def _build_mermaid(phases: list[dict[str, Any]]) -> str:
    """Build a Mermaid graph definition from orchestration phases."""
    lines = ["graph TD"]
    for phase in phases:
        phase_id = phase.get("id", 0)
        phase_name = phase.get("name", f"Phase {phase_id}")
        node_id = f"P{phase_id}"
        lines.append(f'    {node_id}["{phase_name}"]')
        for dep_id in phase.get("depends_on", []):
            lines.append(f"    P{dep_id} --> {node_id}")
        for task in phase.get("tasks", []):
            task_name = task.get("name", "?")
            task_node = f"{node_id}_{task.get('config_id', 'x')}"
            icon = task.get("type_icon", "")
            label = f"{icon} {task_name}" if icon else task_name
            lines.append(f'    {task_node}["{label}"]')
            lines.append(f"    {node_id} --> {task_node}")
    return "\n".join(lines)


def _type_icon(component_id: str) -> str:
    """Map component ID to a short type icon."""
    if ".ex-" in component_id or component_id.startswith("ex-"):
        return "EX"
    if ".wr-" in component_id or component_id.startswith("wr-"):
        return "WR"
    if "transformation" in component_id or "snowflake-sql" in component_id:
        return "TR"
    if "orchestrator" in component_id:
        return "OT"
    return "AP"


class ExplorerService(BaseService):
    """Generates catalog and orchestration data for the kbc-explorer HTML app.

    Collects configs, jobs, lineage, and orchestration details from all
    registered projects and assembles them into the schema expected by
    the explorer's index.html.
    """

    def __init__(
        self,
        config_store: ConfigStore,
        config_service: ConfigService,
        job_service: JobService,
        lineage_service: LineageService,
        client_factory: ClientFactory | None = None,
    ) -> None:
        super().__init__(config_store=config_store, client_factory=client_factory)
        self._config_service = config_service
        self._job_service = job_service
        self._lineage_service = lineage_service

    def init_tiers(self, output_path: Path) -> dict[str, Any]:
        """Generate a tiers.yaml template from registered projects.

        Auto-detects tier from alias naming convention (-l0-, -l1-, -l2-)
        and marks unclassified projects with a comment.

        Args:
            output_path: Where to write the YAML file.

        Returns:
            Dict with generation summary.
        """
        projects = self.resolve_projects(None)
        if not projects:
            raise ConfigError("No projects configured. Use 'kbagent project add' first.")

        tier_lines = [
            "# Tier configuration for KBC Explorer",
            "# Generated by: kbagent explorer init-tiers",
            "#",
            "# Tiers classify projects by their role in the data pipeline:",
            "#   L0 = Data Sources / Extraction",
            "#   L1 = Processing / Transformation",
            "#   L2 = Output / Delivery",
            "#",
            "# Edit this file and use it with: kbagent explorer --tiers tiers.yaml",
            "",
            "description: \"Project catalog\"",
            "",
            "tiers:",
            "  L0:",
            "    name: \"Data Sources / Extraction\"",
            "    description: \"Raw data extraction from external systems\"",
            "  L1:",
            "    name: \"Processing / Transformation\"",
            "    description: \"Data processing, transformation, and modeling\"",
            "  L2:",
            "    name: \"Output / Delivery\"",
            "    description: \"Final data products, dashboards, and data sharing\"",
            "",
            "projects:",
        ]

        classified = 0
        unclassified = 0
        for alias in sorted(projects.keys()):
            tier, was_unclassified = _assign_tier(alias)
            if was_unclassified:
                tier_lines.append(f"  {alias}: L0  # TODO: assign correct tier")
                unclassified += 1
            else:
                tier_lines.append(f"  {alias}: {tier}")
                classified += 1

        tier_lines.append("")  # trailing newline

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("\n".join(tier_lines))

        return {
            "output_path": str(output_path),
            "total_projects": len(projects),
            "auto_classified": classified,
            "unclassified": unclassified,
        }

    def generate(
        self,
        aliases: list[str] | None = None,
        output_dir: Path | None = None,
        job_limit: int = MAX_JOB_LIMIT,
        open_browser: bool = True,
        tiers_config: Path | None = None,
    ) -> dict[str, Any]:
        """Generate explorer data files and optionally open the browser.

        Args:
            aliases: Project aliases to include. None means all.
            output_dir: Directory to write files. Defaults to kbc-explorer/.
            job_limit: Max jobs per project for stats.
            open_browser: Whether to open index.html after generation.
            tiers_config: Path to YAML tier config file.

        Returns:
            Dict with generation summary and any errors.
        """
        if output_dir is None:
            output_dir = _default_output_dir()

        projects = self.resolve_projects(aliases)
        if not projects:
            raise ConfigError("No projects configured. Use 'kbagent project add' first.")

        all_errors: list[dict[str, str]] = []

        # Step 1: Collect configs
        logger.info("Collecting configurations from %d projects...", len(projects))
        config_result = self._config_service.list_configs(
            aliases=list(projects.keys())
        )
        all_errors.extend(config_result.get("errors", []))

        # Step 2: Collect jobs
        logger.info("Collecting job history from %d projects...", len(projects))
        job_result = self._job_service.list_jobs(
            aliases=list(projects.keys()), limit=job_limit
        )
        all_errors.extend(job_result.get("errors", []))

        # Step 3: Collect lineage
        logger.info("Collecting lineage from %d projects...", len(projects))
        lineage_result = self._lineage_service.get_lineage(
            aliases=list(projects.keys())
        )
        all_errors.extend(lineage_result.get("errors", []))

        # Step 4: Group data by project
        configs_by_project: dict[str, list[dict[str, Any]]] = {}
        for cfg in config_result.get("configs", []):
            alias = cfg["project_alias"]
            configs_by_project.setdefault(alias, []).append(cfg)

        jobs_by_project: dict[str, list[dict[str, Any]]] = {}
        for job in job_result.get("jobs", []):
            alias = job["project_alias"]
            jobs_by_project.setdefault(alias, []).append(job)

        # Build sharing_out/sharing_in from lineage edges
        sharing_out_by_project: dict[str, list[dict[str, Any]]] = {}
        sharing_in_by_project: dict[str, list[dict[str, Any]]] = {}
        for edge in lineage_result.get("edges", []):
            src_alias = edge.get("source_project_alias", "")
            tgt_alias = edge.get("target_project_alias", "")
            if src_alias:
                sharing_out_by_project.setdefault(src_alias, []).append({
                    "bucket": edge.get("source_bucket_id", ""),
                    "target_project": tgt_alias,
                    "target_project_name": edge.get("target_project_name", ""),
                    "target_bucket": edge.get("target_bucket_id", ""),
                    "sharing_type": edge.get("sharing_type", ""),
                })
            if tgt_alias:
                sharing_in_by_project.setdefault(tgt_alias, []).append({
                    "bucket": edge.get("target_bucket_id", ""),
                    "source_project": src_alias,
                    "source_project_name": edge.get("source_project_name", ""),
                    "source_bucket": edge.get("source_bucket_id", ""),
                    "sharing_type": edge.get("sharing_type", ""),
                })

        # Load tier config if provided
        tier_map: dict[str, str] | None = None
        tier_descriptions: dict[str, dict[str, str]] | None = None
        catalog_description: str | None = None
        if tiers_config is not None:
            tier_map, tier_descriptions, catalog_description = self._load_tiers_config(
                tiers_config, all_errors
            )

        # Step 5: Assemble per-project data
        project_data: dict[str, dict[str, Any]] = {}
        tiers: dict[str, list[str]] = {"L0": [], "L1": [], "L2": []}

        for alias, project in projects.items():
            tier, was_unclassified = _assign_tier(alias, tier_map)
            if was_unclassified:
                all_errors.append({
                    "project_alias": alias,
                    "error_code": "TIER_UNCLASSIFIED",
                    "message": f"Project '{alias}' has no tier mapping — defaulting to L0",
                })
            tiers[tier].append(alias)

            # Group configs by type
            configs = configs_by_project.get(alias, [])
            by_type: dict[str, dict[str, Any]] = {}
            for cfg in configs:
                ctype = cfg.get("component_type", "other")
                if ctype not in by_type:
                    by_type[ctype] = {"count": 0, "configs": []}
                by_type[ctype]["count"] += 1
                by_type[ctype]["configs"].append({
                    "config_id": cfg["config_id"],
                    "config_name": cfg["config_name"],
                    "config_description": cfg.get("config_description", ""),
                    "component_id": cfg["component_id"],
                    "component_name": cfg.get("component_name", ""),
                })

            jobs = jobs_by_project.get(alias, [])
            job_stats = _compute_job_stats(jobs)

            project_data[alias] = {
                "alias": alias,
                "name": project.project_name or alias,
                "project_id": project.project_id or 0,
                "tier": tier,
                "configurations": {
                    "total_configs": len(configs),
                    "by_type": by_type,
                },
                "job_stats": job_stats,
                "sharing_out": sharing_out_by_project.get(alias, []),
                "sharing_in": sharing_in_by_project.get(alias, []),
            }

        # Step 6: Collect orchestrations
        orchestrations = self._collect_orchestrations(
            configs_by_project, all_errors
        )

        # Step 7: Build lineage for catalog
        lineage_edges = []
        for edge in lineage_result.get("edges", []):
            lineage_edges.append({
                "source_project_alias": edge.get("source_project_alias", ""),
                "source_project_id": str(edge.get("source_project_id", "")),
                "source_project_name": edge.get("source_project_name", ""),
                "source_bucket_id": edge.get("source_bucket_id", ""),
                "target_project_alias": edge.get("target_project_alias", ""),
                "target_project_id": str(edge.get("target_project_id", "")),
                "target_project_name": edge.get("target_project_name", ""),
                "target_bucket_id": edge.get("target_bucket_id", ""),
                "sharing_type": edge.get("sharing_type", ""),
            })

        sharing_out_count = len({
            e.get("source_project_alias")
            for e in lineage_result.get("edges", [])
            if e.get("source_project_alias")
        })
        receiving_in_count = len({
            e.get("target_project_alias")
            for e in lineage_result.get("edges", [])
            if e.get("target_project_alias")
        })

        # Determine stack_url from first project
        first_project = next(iter(projects.values()))
        stack_url = first_project.stack_url

        # Step 8: Assemble catalog
        default_tier_defs = {
            "L0": {
                "name": "Data Sources / Extraction",
                "description": "Raw data extraction from external systems",
            },
            "L1": {
                "name": "Processing / Transformation",
                "description": "Data processing and transformation",
            },
            "L2": {
                "name": "Output / Delivery",
                "description": "Final data products and delivery",
            },
        }
        # Merge tier descriptions from config file if provided
        if tier_descriptions:
            for tier_key, tier_def in tier_descriptions.items():
                if tier_key in default_tier_defs:
                    default_tier_defs[tier_key].update(tier_def)

        description = catalog_description or f"Project catalog with {len(projects)} projects"

        catalog = {
            "metadata": {
                "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "tool": f"kbagent CLI v{__version__}",
                "stack_url": stack_url,
                "description": description,
            },
            "tiers": {
                tier_key: {
                    **default_tier_defs.get(tier_key, {"name": tier_key, "description": ""}),
                    "projects": sorted(tier_projects),
                }
                for tier_key, tier_projects in tiers.items()
            },
            "projects": project_data,
            "lineage": {
                "edges": lineage_edges,
                "summary": {
                    "total_edges": len(lineage_edges),
                    "projects_sharing_out": sharing_out_count,
                    "projects_receiving_in": receiving_in_count,
                },
            },
            "orchestrations": orchestrations,
        }

        # Step 8b: Schema validation
        schema_path = Path(output_dir) / "schema.json"
        if schema_path.exists():
            try:
                schema = json.loads(schema_path.read_text())
                jsonschema.validate(catalog, schema)
                logger.info("Catalog passed schema validation")
            except jsonschema.ValidationError as exc:
                logger.warning("Catalog schema validation failed: %s", exc.message)
                all_errors.append({
                    "project_alias": "_schema",
                    "error_code": "SCHEMA_VALIDATION_ERROR",
                    "message": f"Schema validation: {exc.message}",
                })
            except (json.JSONDecodeError, jsonschema.SchemaError) as exc:
                logger.warning("Failed to load/parse schema: %s", exc)
                all_errors.append({
                    "project_alias": "_schema",
                    "error_code": "SCHEMA_LOAD_ERROR",
                    "message": f"Failed to load schema: {exc}",
                })

        # Step 9: Write files (atomic: write .tmp then os.replace)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        catalog_json_path = output_dir / "catalog.json"
        catalog_js_path = output_dir / "catalog.js"

        catalog_json_str = json.dumps(catalog, indent=2)

        for path, content in [
            (catalog_json_path, catalog_json_str),
            (catalog_js_path, f"const CATALOG = {catalog_json_str};\n"),
        ]:
            tmp_path = path.with_suffix(path.suffix + ".tmp")
            tmp_path.write_text(content)
            os.replace(tmp_path, path)

        logger.info("Wrote catalog and orchestration files to %s", output_dir)

        # Step 10: Open browser
        index_path = output_dir / "index.html"
        if open_browser and index_path.exists():
            webbrowser.open(index_path.as_uri())

        return {
            "output_dir": str(output_dir),
            "projects_count": len(projects),
            "configs_count": sum(
                p["configurations"]["total_configs"] for p in project_data.values()
            ),
            "jobs_sampled": sum(
                p["job_stats"]["total_jobs"] for p in project_data.values()
            ),
            "lineage_edges": len(lineage_edges),
            "orchestrations_count": len(orchestrations),
            "errors": all_errors,
            "files_written": [
                str(catalog_json_path),
                str(catalog_js_path),
            ],
        }

    @staticmethod
    def _load_tiers_config(
        tiers_config: Path,
        all_errors: list[dict[str, str]],
    ) -> tuple[dict[str, str] | None, dict[str, dict[str, str]] | None, str | None]:
        """Load a YAML tier config file.

        Returns:
            Tuple of (project_tier_map, tier_descriptions, catalog_description).
            Any or all may be None if loading fails.
        """
        try:
            data = yaml.safe_load(tiers_config.read_text())
        except Exception as exc:
            all_errors.append({
                "project_alias": "_tiers",
                "error_code": "TIERS_CONFIG_ERROR",
                "message": f"Failed to load tiers config: {exc}",
            })
            return None, None, None

        if not isinstance(data, dict):
            all_errors.append({
                "project_alias": "_tiers",
                "error_code": "TIERS_CONFIG_ERROR",
                "message": "Tiers config must be a YAML mapping",
            })
            return None, None, None

        project_map: dict[str, str] = {}
        for alias, tier in data.get("projects", {}).items():
            project_map[str(alias)] = str(tier).upper()

        tier_descriptions: dict[str, dict[str, str]] = {}
        for tier_key, tier_def in data.get("tiers", {}).items():
            if isinstance(tier_def, dict):
                tier_descriptions[str(tier_key).upper()] = {
                    k: str(v) for k, v in tier_def.items()
                }

        catalog_description = data.get("description")
        if catalog_description:
            catalog_description = str(catalog_description)

        return project_map, tier_descriptions, catalog_description

    def _collect_orchestrations(
        self,
        configs_by_project: dict[str, list[dict[str, Any]]],
        all_errors: list[dict[str, str]],
    ) -> dict[str, Any]:
        """Collect orchestration details for all keboola.orchestrator configs.

        Uses ThreadPoolExecutor to fetch all orchestration details in parallel.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        orchestrations: dict[str, Any] = {}

        # Build list of (alias, cfg) tuples for all orchestrator configs
        orch_items: list[tuple[str, dict[str, Any]]] = []
        for alias, configs in configs_by_project.items():
            for cfg in configs:
                if cfg.get("component_id") == "keboola.orchestrator":
                    orch_items.append((alias, cfg))

        if not orch_items:
            return orchestrations

        def _fetch_one(alias: str, cfg: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
            config_id = cfg["config_id"]
            detail = self._config_service.get_config_detail(
                alias=alias,
                component_id="keboola.orchestrator",
                config_id=config_id,
            )
            parsed = self._parse_orchestration(alias, config_id, cfg, detail)
            return alias, config_id, parsed

        max_workers = min(len(orch_items), self._resolve_max_workers())
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_fetch_one, alias, cfg): (alias, cfg)
                for alias, cfg in orch_items
            }
            for future in as_completed(futures):
                alias, cfg = futures[future]
                config_id = cfg["config_id"]
                try:
                    _, _, parsed = future.result()
                    orchestrations[f"{alias}|{config_id}"] = parsed
                except Exception as exc:
                    logger.warning(
                        "Failed to fetch orchestration %s/%s: %s", alias, config_id, exc
                    )
                    all_errors.append({
                        "project_alias": alias,
                        "error_code": "ORCHESTRATION_ERROR",
                        "message": f"Failed to fetch flow {config_id}: {exc}",
                    })

        return orchestrations

    @staticmethod
    def _parse_orchestration(
        alias: str,
        config_id: str,
        cfg: dict[str, Any],
        detail: dict[str, Any],
    ) -> dict[str, Any]:
        """Parse an orchestration config detail into the explorer format.

        The Keboola Flow API stores phases and tasks as separate arrays:
        - ``configuration.phases``: list of {id, name, dependsOn}
        - ``configuration.tasks``: list of {id, name, phase, task: {componentId, configId}, ...}

        Tasks reference their phase via ``task["phase"] == phase["id"]``.
        ``dependsOn`` is a list of raw phase-id integers (not objects).
        """
        config_data = detail.get("configuration", {})
        phases_raw = config_data.get("phases", [])
        tasks_raw = config_data.get("tasks", [])

        # Index tasks by their parent phase id
        tasks_by_phase: dict[int | str, list[dict[str, Any]]] = {}
        for task in tasks_raw:
            phase_id = task.get("phase")
            tasks_by_phase.setdefault(phase_id, []).append(task)

        phases = []
        for phase in phases_raw:
            phase_id = phase.get("id", 0)
            phase_tasks = tasks_by_phase.get(phase_id, [])
            tasks = []
            for task in phase_tasks:
                task_cfg = task.get("task", {})
                # task_cfg may be a dict or a plain string; guard against both
                if not isinstance(task_cfg, dict):
                    task_cfg = {}
                comp_id = task_cfg.get("componentId", "")
                tasks.append({
                    "name": task.get("name", ""),
                    "component_id": comp_id,
                    "component_short": comp_id.split(".")[-1] if comp_id else "",
                    "config_id": str(task_cfg.get("configId", task_cfg.get("configurationId", ""))),
                    "enabled": task.get("enabled", True),
                    "continue_on_failure": task.get("continueOnFailure", False),
                    "type_icon": _type_icon(comp_id),
                })

            # dependsOn is a list of raw integers (phase ids)
            depends_on_raw = phase.get("dependsOn", [])
            depends_on = [
                d.get("phaseId", d) if isinstance(d, dict) else d
                for d in depends_on_raw
            ]

            phases.append({
                "id": phase_id,
                "name": phase.get("name", ""),
                "depends_on": depends_on,
                "tasks": tasks,
            })

        total_tasks = sum(len(p["tasks"]) for p in phases)

        result = {
            "project_alias": alias,
            "config_id": config_id,
            "name": cfg.get("config_name", detail.get("name", "")),
            "description": cfg.get("config_description", detail.get("description", "")),
            "is_disabled": detail.get("isDisabled", False),
            "version": detail.get("version", 0),
            "last_modified": detail.get("changeDescription", ""),
            "last_modified_by": "",
            "phases": phases,
            "total_tasks": total_tasks,
            "total_phases": len(phases),
            "mermaid": _build_mermaid(phases),
        }
        return result
