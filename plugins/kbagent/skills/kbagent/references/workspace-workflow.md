# Workspace Workflow -- SQL Debugging

Workspaces let you run SQL against real project data without triggering full Keboola jobs.
Use this to debug failing transformations iteratively.

## Option A: Debug an existing transformation

Best when you have a failing transformation and want to reproduce the error:

```bash
# Step 1: Create workspace from transformation (auto-loads input tables)
kbagent --json workspace from-transformation \
  --project ALIAS \
  --component-id keboola.snowflake-transformation \
  --config-id CONFIG_ID

# Response includes: workspace_id, host, database, schema, user, password
# SAVE THE PASSWORD -- it cannot be retrieved later!
```

```bash
# Step 2: Run the original SQL to reproduce the error
kbagent --json workspace query \
  --project ALIAS \
  --workspace-id WS_ID \
  --sql "SELECT ..."
```

```bash
# Step 3: Iterate on fixes (no need to run full jobs)
kbagent --json workspace query \
  --project ALIAS \
  --workspace-id WS_ID \
  --sql "SELECT fixed_query ..."
```

```bash
# Step 4: Once the fix works, update the transformation config via MCP
kbagent --json tool call update_configuration \
  --project ALIAS \
  --input '{"component_id": "keboola.snowflake-transformation", "configuration_id": "CONFIG_ID", ...}'
```

```bash
# Step 5: Clean up (optional -- workspaces expire automatically)
kbagent --json workspace delete --project ALIAS --workspace-id WS_ID
```

## Option B: Ad-hoc workspace

Best when you want to explore data or run arbitrary queries:

```bash
# Create empty workspace
kbagent --json workspace create --project ALIAS --name "debug-ws"

# Load specific tables
kbagent --json workspace load \
  --project ALIAS \
  --workspace-id WS_ID \
  --tables in.c-bucket.table1 \
  --tables in.c-bucket.table2

# Query
kbagent --json workspace query \
  --project ALIAS \
  --workspace-id WS_ID \
  --sql "SELECT * FROM \"table1\" LIMIT 10"
```

## Option C: UI-visible workspace

Use `--ui` when the workspace should appear in the Keboola UI Workspaces tab (slower, ~15s):

```bash
kbagent --json workspace create --project ALIAS --name "shared-debug" --ui
```

## SQL from file

For multi-line SQL, use `--file` instead of `--sql`:

```bash
kbagent --json workspace query \
  --project ALIAS \
  --workspace-id WS_ID \
  --file query.sql
```

## Key details

- **Password**: only returned on creation (headless) or after `workspace password` (reset)
- **Expiration**: workspaces expire server-side automatically
- **Quoting**: Snowflake identifiers with hyphens need double quotes: `"my-table"`
- **Query Service**: uses Storage API token for auth -- no Snowflake credentials needed in the query command
- **Transactional mode**: add `--transactional` to wrap SQL in a transaction
