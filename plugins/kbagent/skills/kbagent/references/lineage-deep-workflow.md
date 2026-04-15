# Lineage Workflow -- Column-Level Lineage Analysis

Lineage deep builds a full dependency graph from sync'd project data on disk.
It detects table-level and column-level relationships by parsing config files,
SQL code, and optionally using AI for ambiguous mappings.

## Prerequisites

Projects must be sync'd to disk before building lineage:

```bash
# Pull all projects (or a specific one)
kbagent sync pull --all-projects
```

The sync'd directory structure is the input for lineage analysis.

## Build lineage

```bash
# Build from sync'd data and save to cache file
kbagent lineage build -d /path/to/sync-dir -o lineage.json
```

This scans all project directories, parses configs, and writes the full
lineage graph to `lineage.json` for fast subsequent queries.

## Query from cache

Once built, query the graph without re-scanning:

```bash
# Show downstream dependencies of a table
kbagent lineage show -l lineage.json --downstream "my-project:in.c-main.users"

# Show upstream dependencies
kbagent lineage show -l lineage.json --upstream "my-project:out.c-analytics.report"
```

## Column-level detail

```bash
# Show column-level mappings for all tables in the result
kbagent lineage show -l lineage.json --downstream "my-project:in.c-main.users" --columns

# Trace a single column through the lineage
kbagent lineage show -l lineage.json --downstream "my-project:in.c-main.users" -c user_id
```

## Refresh in one step

Combine sync pull + rebuild into a single command:

```bash
kbagent lineage build -d /path/to/sync-dir -o lineage.json --refresh
```

This runs `sync pull` first, then rebuilds the lineage graph.

## AI-enhanced analysis

For ambiguous SQL or Python code where deterministic parsing cannot resolve
column mappings, use the `--ai` flag:

```bash
kbagent lineage build -d /path/to/sync-dir -o lineage.json --ai
```

AI analysis uses the Claude CLI haiku model and caches results keyed by
code hash. Changed code is re-analyzed on the next `--ai` run; unchanged
code uses the cached result.

## Node identifiers

Tables are identified by fully-qualified names (FQN):

| Format | Example | Notes |
|--------|---------|-------|
| Full FQN | `my-project:in.c-main.users` | Always unambiguous |
| Table only | `in.c-main.users` | Auto-resolves; warns if ambiguous across projects |

Use the full FQN (`project-alias:bucket_id.table_name`) when multiple
projects contain tables with the same name.

## Detection methods

| Method | Source | Type |
|--------|--------|------|
| `input_mapping` | Config input mapping definitions | Deterministic |
| `output_mapping` | Config output mapping definitions | Deterministic |
| `sql_tokenizer` | SQL parsing for Snowflake table references | Deterministic |
| `bucket_sharing` | Cross-project sharing from `kbagent lineage show` | Deterministic |
| `sql_ai` | AI analysis of SQL code | Requires `--ai` |
| `python_ai` | AI analysis of Python code | Requires `--ai` |

## Key details

- **Non-sync'd projects**: cross-project references to projects not in the sync
  directory appear as `unknown-{project_id}` in the graph
- **AI cache**: keyed by code hash -- only changed code is re-analyzed on
  subsequent `--ai` runs
- **`--refresh`**: runs sync pull before rebuilding, so the graph reflects the
  latest remote state
- **Cache file**: the `-o` / `-l` JSON file is the single source of truth for
  queries; rebuild it when project configs change
