# PLAN: `kbagent catalog generate` — Automated Catalog Generation

## Problem Statement

KBC Explorer (`kbc-explorer/index.html`) is a powerful CDO-grade dashboard for
visualizing a Keboola project ecosystem. However, its data file — `catalog.json`
(including orchestrations) — is currently produced through a **manual 7-step
process** described in `kbc-explorer/README.md`, typically executed by giving a
long prompt to an AI assistant.

This is unsustainable because:
- **No reproducibility** — each generation run depends on AI interpretation.
- **No scheduling** — data goes stale; a CDO needs daily/weekly refreshes.
- **No validation** — manual assembly can produce schema-violating output.
- **No diffing** — no way to compare two snapshots and see what changed.
- **Slow** — a human-in-the-loop process takes 10-20 minutes per run.

## Goal

A single CLI command that produces a complete, schema-valid catalog snapshot:

```bash
# Generate catalog from all registered projects
kbagent catalog generate --output kbc-explorer/

# With custom tier configuration
kbagent catalog generate --tiers tiers.yaml --output kbc-explorer/

# Dry run — show what would be fetched without writing files
kbagent catalog generate --dry-run

# JSON output of the catalog (pipe-friendly)
kbagent --json catalog generate
```

Expected output files:
- `catalog.json` — plain JSON (for tooling, validation), includes orchestrations under the `orchestrations` key
- `catalog.js` — JS wrapper (`const CATALOG = {...};`) for the HTML viewer

---

## Architecture

### New Files

```
src/keboola_agent_cli/
  commands/catalog.py           # LAYER 1: CLI command
  services/catalog_service.py   # LAYER 2: Orchestration + aggregation logic
```

### Layer Responsibilities

Following the existing 3-layer architecture:

```
commands/catalog.py     →  CatalogService     →  KeboolaClient
  CLI args, output           Orchestration          HTTP calls
                             Aggregation
                             Schema validation
```

`CatalogService` reuses the existing service layer:
- `ConfigService.list_configs()` — configuration data
- `JobService.list_jobs()` — job data (raw, for aggregation)
- `LineageService.get_lineage()` — sharing/lineage data
- `ConfigService.get_config_detail()` — orchestration flow details

No new API endpoints are needed. All data is already fetchable.

---

## Tier Configuration

Tiers (L0/L1/L2) cannot be inferred from the Keboola API — they represent a
business-level classification. The user must provide a tier mapping.

### Option A: YAML config file (recommended)

```yaml
# tiers.yaml
description: "IR (Internal Reporting) project ecosystem"

tiers:
  L0:
    name: "Data Sources / Extraction"
    description: "Raw data extraction from external systems"
  L1:
    name: "Data Processing / Transformation"
    description: "Business logic, transformations, data modeling"
  L2:
    name: "Data Output / Delivery"
    description: "Final outputs - BI dashboards, data sharing"

# Mapping: project alias -> tier
projects:
  ir-l0-finance: L0
  ir-l0-kbc-telemetry-to-catalog: L0
  ir-l0-marketing: L0
  ir-l1-data-processes-product: L1
  ir-l2-internal-bi-output: L2
  # ...
```

### Option B: Convention-based auto-detection

If no tier config is provided, attempt to infer tier from alias patterns:
- `*-l0-*` or `*-l0` → L0
- `*-l1-*` or `*-l1` → L1
- `*-l2-*` or `*-l2` → L2
- Everything else → a configurable default tier or `UNCLASSIFIED`

### Decision

Support **both**: auto-detection as fallback, `--tiers` file as override. If a
project appears in `--tiers` file, use that. If not, try alias-based convention.
If neither works, assign `UNCLASSIFIED` and emit a warning.

---

## Data Collection Pipeline

### Phase 1: Parallel fetch (per-project)

Uses `BaseService._run_parallel()` with `ThreadPoolExecutor`:

```
For each registered project (in parallel):
  ├── 1. config list          → raw component/config data
  ├── 2. job list --limit 500 → raw job data
  └── 3. lineage              → sharing_out, sharing_in, edges
```

All three calls per project can also run in parallel (they are independent),
but the current `_run_parallel` pattern groups by project. This is fine for
the initial implementation. A future optimization could issue all three per
project concurrently via `asyncio` or nested thread pools.

### Phase 2: Orchestration detail fetch (selective, parallel)

From Phase 1 config data, identify all `keboola.orchestrator` configurations.
Then fetch their details in parallel:

```
For each orchestrator config across all projects (in parallel):
  └── config detail (component_id=keboola.orchestrator, config_id=X)
      → phase/task structure, mermaid graph
```

### Phase 3: Aggregation (local, CPU-bound)

No API calls. Pure computation:

```
For each project:
  ├── Configurations: group by type, count totals
  ├── Job stats: compute aggregates from raw jobs
  │   ├── total_jobs
  │   ├── status_counts (success, error, warning, cancelled, terminated)
  │   ├── success_rate_pct = success / total * 100
  │   ├── avg_duration_seconds = sum(durations) / total
  │   ├── date_range (earliest, latest timestamps)
  │   ├── component_stats (per component_id: success, error, other, total)
  │   └── failing_configs (configs with error_rate > 0, sorted desc)
  ├── Sharing: already structured from lineage service
  └── Tier: look up from tiers config or infer from alias

Global:
  ├── lineage.edges: deduplicated cross-project edges
  └── lineage.summary: count aggregates
```

### Phase 4: Assembly & Validation

```
1. Assemble catalog.json structure (metadata + tiers + projects + lineage + orchestrations)
2. Validate against schema.json (using jsonschema library)
3. Write output files:
   ├── catalog.json
   └── catalog.js    (wrapped: const CATALOG = ...;)
```

---

## Job Statistics Aggregation Logic

This is the most complex transformation. Given raw jobs from Queue API:

```python
def aggregate_job_stats(jobs: list[dict]) -> dict:
    """
    Input: raw job list from Queue API
    Each job has: status, durationSeconds, createdTime, component.id,
                  config.id, etc.

    Output: job_stats object matching schema.json#/definitions/job_stats
    """
    if not jobs:
        return {
            "total_jobs": 0,
            "status_counts": {},
            "success_rate_pct": 0,
            "avg_duration_seconds": 0,
            "date_range": {"earliest": None, "latest": None},
            "component_stats": {},
            "failing_configs": []
        }

    # Status counts
    status_counts = Counter(j["status"] for j in jobs)
    total = len(jobs)
    success = status_counts.get("success", 0)

    # Success rate
    success_rate = (success / total * 100) if total > 0 else 0

    # Average duration
    durations = [j.get("durationSeconds", 0) for j in jobs if j.get("durationSeconds")]
    avg_duration = sum(durations) / len(durations) if durations else 0

    # Date range
    timestamps = [j["createdTime"] for j in jobs if j.get("createdTime")]
    earliest = min(timestamps) if timestamps else None
    latest = max(timestamps) if timestamps else None

    # Component stats
    comp_stats = defaultdict(lambda: {"success": 0, "error": 0, "other": 0, "total": 0})
    for j in jobs:
        comp_id = j.get("component", {}).get("id", "unknown")
        comp_stats[comp_id]["total"] += 1
        if j["status"] == "success":
            comp_stats[comp_id]["success"] += 1
        elif j["status"] == "error":
            comp_stats[comp_id]["error"] += 1
        else:
            comp_stats[comp_id]["other"] += 1

    # Failing configs
    config_runs = defaultdict(lambda: {"error": 0, "total": 0, "last_run": None, "component_id": ""})
    for j in jobs:
        comp_id = j.get("component", {}).get("id", "unknown")
        cfg_id = j.get("config", {}).get("id", "unknown")
        key = f"{comp_id}/{cfg_id}"
        config_runs[key]["total"] += 1
        config_runs[key]["component_id"] = comp_id
        if j["status"] == "error":
            config_runs[key]["error"] += 1
        ts = j.get("createdTime")
        if ts and (not config_runs[key]["last_run"] or ts > config_runs[key]["last_run"]):
            config_runs[key]["last_run"] = ts

    failing = [
        {
            "config_key": key,
            "component_id": data["component_id"],
            "error_count": data["error"],
            "total_runs": data["total"],
            "error_rate_pct": round(data["error"] / data["total"] * 100, 1),
            "last_run": data["last_run"]
        }
        for key, data in config_runs.items()
        if data["error"] > 0
    ]
    failing.sort(key=lambda x: x["error_rate_pct"], reverse=True)

    return {
        "total_jobs": total,
        "status_counts": dict(status_counts),
        "success_rate_pct": round(success_rate, 1),
        "avg_duration_seconds": round(avg_duration),
        "date_range": {"earliest": earliest, "latest": latest},
        "component_stats": dict(comp_stats),
        "failing_configs": failing
    }
```

---

## Orchestration Assembly Logic

For each `keboola.orchestrator` config, the detail response contains phases
and tasks. The assembly transforms this into the `orchestrations` entry format:

```python
def assemble_orchestration(alias: str, config_detail: dict) -> dict:
    """
    Transform config detail response into orchestration catalog entry.

    Input: raw response from client.get_config_detail()
    Output: orchestration entry with phases, tasks, mermaid graph
    """
    config = config_detail
    rows = config.get("rows", [])  # phases come from config rows or configuration.phases

    # Extract phases and tasks from the orchestrator configuration
    phases = config.get("configuration", {}).get("phases", [])

    assembled_phases = []
    for phase in phases:
        tasks = []
        for task in phase.get("tasks", []):
            comp_id = task.get("task", {}).get("componentId", "")
            comp_short = comp_id.replace("keboola.", "").replace("ex-generic-v2", "generic-extractor")
            type_map = {
                "extractor": "EX", "writer": "WR",
                "transformation": "TR", "application": "AP"
            }
            # Determine type icon from component type
            type_icon = type_map.get(task.get("task", {}).get("type", ""), "OT")

            tasks.append({
                "name": task.get("name", ""),
                "component_id": comp_id,
                "component_short": comp_short,
                "config_id": task.get("task", {}).get("configId", ""),
                "enabled": task.get("enabled", True),
                "continue_on_failure": task.get("continueOnFailure", False),
                "type_icon": type_icon
            })

        assembled_phases.append({
            "id": phase.get("id", 0),
            "name": phase.get("name", f"Phase {phase.get('id', '?')}"),
            "depends_on": phase.get("dependsOn", []),
            "tasks": tasks
        })

    total_tasks = sum(len(p["tasks"]) for p in assembled_phases)

    # Generate Mermaid graph
    mermaid = generate_mermaid(assembled_phases)

    return {
        "project_alias": alias,
        "config_id": config.get("id", ""),
        "name": config.get("name", ""),
        "description": config.get("description", ""),
        "is_disabled": config.get("isDisabled", False),
        "version": config.get("version", 0),
        "last_modified": config.get("changeDescription", ""),
        "last_modified_by": config.get("creatorToken", {}).get("description", ""),
        "phases": assembled_phases,
        "total_tasks": total_tasks,
        "total_phases": len(assembled_phases),
        "mermaid": mermaid
    }
```

---

## CLI Command Interface

### `kbagent catalog generate`

```
Usage: kbagent catalog generate [OPTIONS]

  Generate catalog.json (with orchestrations) for KBC Explorer.

Options:
  --output DIR          Output directory (default: ./kbc-explorer/)
  --tiers FILE          Tier configuration YAML file (optional)
  --job-limit N         Max jobs to fetch per project (default: 500)
  --skip-orchestrations Skip fetching orchestration details (faster)
  --validate-only       Only validate existing catalog against schema
  --dry-run             Show what would be fetched, don't write files
  --project ALIAS       Only generate for specific project(s) (repeatable)
```

### `kbagent catalog validate`

```
Usage: kbagent catalog validate [OPTIONS]

  Validate catalog.json against the JSON schema.

Options:
  --catalog FILE   Path to catalog.json (default: ./kbc-explorer/catalog.json)
  --schema FILE    Path to schema.json (default: ./kbc-explorer/schema.json)
```

### `kbagent catalog diff`

```
Usage: kbagent catalog diff [OPTIONS] OLD_CATALOG NEW_CATALOG

  Compare two catalog snapshots and show what changed.

Options:
  --format TEXT    Output format: text, json (default: text)

Output:
  - New/removed projects
  - Config count changes per project
  - Success rate changes
  - New/removed lineage edges
  - New/removed orchestrations
```

---

## Implementation Plan

### Step 1: Tier configuration loader

**File:** `src/keboola_agent_cli/services/catalog_service.py`

- Parse `tiers.yaml` if provided
- Fallback to alias-based convention detection (`*-l0-*` → L0)
- Validate that every registered project has a tier assignment
- Emit warnings for unclassified projects

### Step 2: CatalogService core — data fetching

**File:** `src/keboola_agent_cli/services/catalog_service.py`

- `generate(output_dir, tiers_config, job_limit, skip_orchestrations)` — main entry point
- `_fetch_all_projects()` — parallel fetch configs + jobs + lineage for all projects
- `_fetch_orchestrations(orchestrator_configs)` — parallel fetch config details
- Reuse existing services via composition (not inheritance):
  ```python
  class CatalogService:
      def __init__(self, config_store, client_factory):
          self.config_svc = ConfigService(config_store, client_factory)
          self.job_svc = JobService(config_store, client_factory)
          self.lineage_svc = LineageService(config_store, client_factory)
  ```

### Step 3: Aggregation functions

**File:** `src/keboola_agent_cli/services/catalog_service.py`

- `_aggregate_job_stats(raw_jobs)` — compute all job_stats fields
- `_build_configurations(raw_configs)` — group by type, count
- `_build_lineage(all_project_lineage)` — deduplicate edges, compute summary
- `_build_orchestrations(flow_details)` — assemble orchestration entries for catalog

### Step 4: Schema validation

**File:** `src/keboola_agent_cli/services/catalog_service.py`

- Load `kbc-explorer/schema.json`
- Validate assembled catalog against it using `jsonschema` library
- Report all validation errors with JSONPath locations
- New dependency: `jsonschema` (add to pyproject.toml)

### Step 5: Output writers

**File:** `src/keboola_agent_cli/services/catalog_service.py`

- `_write_json(data, path)` — write JSON with consistent formatting
- `_write_js_wrapper(data, variable_name, path)` — write `const X = {...};`
- Write both files atomically (write to .tmp, then rename)

### Step 6: CLI command

**File:** `src/keboola_agent_cli/commands/catalog.py`

- Typer command group: `catalog`
- Subcommands: `generate`, `validate`, `diff`
- Wire into `cli.py` app
- Rich progress display: show per-project progress during fetch
- JSON mode support via `OutputFormatter`

### Step 7: Tests

**File:** `tests/test_catalog_service.py`

- Unit tests for `_aggregate_job_stats()` with various job distributions
- Unit tests for tier assignment (YAML + convention fallback)
- Unit tests for configuration grouping
- Unit tests for lineage edge deduplication
- Integration test: mock all API calls, verify full catalog output matches schema
- Validate that generated catalog.json passes schema.json validation

---

## Future Enhancements (Tier 2 & 3)

### KBC Explorer — Visualization Gaps

These represent CDO questions that the current explorer cannot answer:

| Feature | CDO Question | Data Source |
|---------|-------------|-------------|
| **Data freshness** | "When did jobs last run? Is data current?" | `job_stats.date_range.latest` (already in catalog) + real-time check |
| **Cost/volume metrics** | "How many credits per tier? Costliest project?" | Keboola Telemetry API (new data source) |
| **Change timeline** | "What changed this week? New configs, deleted flows?" | `catalog diff` between snapshots |
| **Alerting rules view** | "Which projects have no monitoring?" | Custom metadata (not in API) |
| **Flow Gantt chart** | "What runs in parallel vs sequential?" | `catalog.orchestrations` phases (already available) |
| **Table-level lineage** | "Where does table X originate and flow to?" | MCP `get_lineage` tool or Storage API metadata |
| **Config parameter audit** | "Which extractors target which external systems?" | Would require config parameter access (security risk) |

### kbagent CLI — Agent Gaps

| Feature | Command | Purpose |
|---------|---------|---------|
| `kbagent catalog watch` | `catalog generate --watch --interval 6h` | Scheduled regeneration with delta detection |
| `kbagent project health` | `project health [--project ALIAS]` | Composite health score: success_rate * freshness * config_coverage |
| `kbagent flow list` | `flow list [--project ALIAS]` | Dedicated flow listing (currently buried in config detail) |
| `kbagent flow run` | `flow run --project ALIAS --flow-id ID` | Trigger flow execution via Queue API |
| `kbagent catalog publish` | `catalog publish --to s3://...` | Upload catalog snapshot to S3/GCS for hosted explorer |

---

## Dependencies

### New Python packages

| Package | Purpose | Already in project? |
|---------|---------|---------------------|
| `jsonschema` | Validate catalog against schema.json | No — add to pyproject.toml |
| `pyyaml` | Parse tiers.yaml config | No — add to pyproject.toml |

### Existing packages (no changes)

- `httpx` — HTTP client (used by KeboolaClient)
- `typer` — CLI framework
- `rich` — Progress bars, tables
- `pydantic` — Models

---

## Estimated Effort

| Step | Description | Size |
|------|-------------|------|
| 1 | Tier config loader + YAML parsing | Small |
| 2 | CatalogService data fetching (reuses existing services) | Medium |
| 3 | Aggregation functions (job stats, lineage, configs) | Medium |
| 4 | Schema validation integration | Small |
| 5 | Output writers (JSON + JS wrappers) | Small |
| 6 | CLI command + Rich progress | Medium |
| 7 | Tests | Medium |
| **Total** | | **~400-600 lines of new code + tests** |

The biggest risk is in Step 3 (aggregation) — the job stats calculation has
many edge cases (empty projects, projects with no jobs, malformed timestamps).
The reference implementation above covers these, but thorough testing is needed.

---

## Success Criteria

1. `kbagent catalog generate` produces identical output to the current manually
   generated `catalog.json` (modulo timestamp and ordering differences).
2. Output passes `jsonschema` validation against `schema.json`.
3. Full generation for 27 projects completes in under 60 seconds.
4. `--dry-run` shows expected API call count without making requests.
5. Tier auto-detection correctly classifies all `ir-l0-*`, `ir-l1-*`, `ir-l2-*`
   projects without a config file.
6. Error in one project does not block generation for others (error accumulation
   pattern, consistent with existing multi-project commands).
