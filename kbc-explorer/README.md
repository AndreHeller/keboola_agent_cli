# KBC Explorer

A standalone HTML viewer for analyzing a Keboola project ecosystem. It provides
an interactive dashboard with project details, configuration inventories, job
health metrics, data lineage graphs, and orchestration (flow) diagrams -- all
rendered client-side from a single static data file.

No backend server is required. The viewer runs entirely in the browser.

## Quick Start

```bash
# Generate data and open the dashboard
kbagent explorer

# Generate without opening the browser
kbagent explorer --no-open

# Generate for specific projects only
kbagent explorer --project prod --project dev --no-open

# With custom tier configuration
kbagent explorer --tiers tiers.yaml
```

The command collects configs, jobs, lineage, and orchestration details from all
registered projects in parallel, assembles `catalog.json` + `catalog.js`, and
opens `index.html` in your default browser.

## File Structure

```
kbc-explorer/
  index.html           # Single-page application (HTML + CSS + JS)
  catalog.js           # Generated data: const CATALOG = { ... }; (gitignored)
  catalog.json         # Same data, plain JSON for tooling (gitignored)
  schema.json          # JSON Schema (draft-07) describing catalog.json structure
  README.md            # This file
```

The `.js` file is what the HTML page loads via a `<script>` tag. The `.json`
file contains the same data without the variable wrapper and is useful for
validation, processing, or feeding into other tools.

Both `catalog.js` and `catalog.json` are generated files and are not committed
to git. Run `kbagent explorer --no-open` to regenerate them.

## Prerequisites

- The `kbagent` CLI is installed (`uv pip install -e .` from the repo root).
- All target projects are registered via `kbagent project add` or
  `kbagent org setup`.
- You can verify registered projects with `kbagent project list`.

## Options

```
kbagent explorer [OPTIONS]

  --project NAME     Project alias(es) to include (repeatable, default: all)
  --output-dir DIR   Directory for output files (default: kbc-explorer/)
  --job-limit N      Max jobs per project for statistics (default: 500)
  --tiers FILE       YAML file mapping project aliases to tiers (L0/L1/L2)
  --no-open          Generate files without opening the browser
```

## Tier Configuration

Tiers (L0/L1/L2) classify projects by their role in the data pipeline. They
can be assigned automatically or via a YAML config file.

### Automatic detection

If no `--tiers` file is provided, tier is inferred from the project alias:
- `*-l0-*` or `l0-*` -> L0 (Data Sources / Extraction)
- `*-l1-*` or `l1-*` -> L1 (Processing / Transformation)
- `*-l2-*` or `l2-*` -> L2 (Output / Delivery)
- Everything else -> L0 with a `TIER_UNCLASSIFIED` warning

### YAML config file

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
  ir-l1-data-processes-product: L1
  ir-l2-internal-bi-output: L2
  keboola-ai: L1
```

Projects in the YAML file override automatic detection.

## JSON Schema

### catalog.json

See `schema.json` for the full JSON Schema (draft-07) definition. The high-level
structure is:

```
{
  "metadata": {
    "generated_at": "ISO-8601 timestamp",
    "tool": "kbagent CLI vX.Y.Z",
    "stack_url": "Keboola connection URL",
    "description": "human-readable description"
  },
  "tiers": {
    "L0": { "name", "description", "projects": ["alias", ...] },
    "L1": { ... },
    "L2": { ... }
  },
  "projects": {
    "alias": {
      "alias": "string",
      "name": "string",
      "project_id": number,
      "tier": "L0|L1|L2",
      "configurations": {
        "total_configs": number,
        "by_type": {
          "extractor|transformation|writer|application|other": {
            "count": number,
            "configs": [
              {
                "config_id": "string",
                "config_name": "string",
                "config_description": "string",
                "component_id": "string",
                "component_name": "string"
              }
            ]
          }
        }
      },
      "job_stats": {
        "total_jobs": number,
        "status_counts": { "success": n, "error": n, ... },
        "success_rate_pct": number,
        "avg_duration_seconds": number,
        "date_range": { "earliest": "ISO-8601", "latest": "ISO-8601" },
        "component_stats": {
          "component.id": {
            "success": n, "error": n, "other": n, "total": n
          }
        },
        "failing_configs": [
          {
            "config_key": "component_id/config_id",
            "component_id": "string",
            "error_count": number,
            "total_runs": number,
            "error_rate_pct": number,
            "last_run": "ISO-8601"
          }
        ]
      },
      "sharing_out": [
        {
          "bucket": "string",
          "target_project": "string",
          "target_project_name": "string",
          "target_bucket": "string",
          "sharing_type": "string"
        }
      ],
      "sharing_in": [
        {
          "bucket": "string",
          "source_project": "string",
          "source_project_name": "string",
          "source_bucket": "string",
          "sharing_type": "string"
        }
      ]
    }
  },
  "lineage": {
    "edges": [
      {
        "source_project_alias": "string",
        "source_project_id": "string",
        "source_project_name": "string",
        "source_bucket_id": "string",
        "target_project_alias": "string",
        "target_project_id": "string",
        "target_project_name": "string",
        "target_bucket_id": "string",
        "sharing_type": "string"
      }
    ],
    "summary": {
      "total_edges": number,
      "projects_sharing_out": number,
      "projects_receiving_in": number
    }
  },
  "orchestrations": {
    "PROJECT_ALIAS|CONFIG_ID": { ... }
  }
}
```

### orchestrations (within catalog.json)

The `orchestrations` key in `catalog.json` is a flat dictionary keyed by
`"PROJECT_ALIAS|CONFIG_ID"`. Each value has:

```
{
  "project_alias": "string",
  "config_id": "string",
  "name": "string",
  "description": "string",
  "is_disabled": boolean,
  "version": number,
  "last_modified": "string",
  "last_modified_by": "string",
  "phases": [
    {
      "id": number,
      "name": "string",
      "depends_on": [phase_id, ...],
      "tasks": [
        {
          "name": "string",
          "component_id": "string",
          "component_short": "string",
          "config_id": "string",
          "enabled": boolean,
          "continue_on_failure": boolean,
          "type_icon": "EX|TR|WR|AP|OT"
        }
      ]
    }
  ],
  "total_tasks": number,
  "total_phases": number,
  "mermaid": "Mermaid graph definition string"
}
```

## Security Notes

- The data files contain only structural metadata: configuration names,
  component types, job statistics, and bucket sharing relationships.
- No API tokens, passwords, connection strings, or other secrets are included.
- Configuration parameters (which may contain credentials) are never exported
  by the `kbagent config list` or `kbagent config detail` commands used in
  the data collection process.
