# Keboola Agent CLI (`kbagent`)

A CLI for managing multiple Keboola projects from one place. Built for AI coding agents (Claude Code, Codex, Gemini) and human operators who need to work across many projects at once.

## Why does this exist?

Keboola's web UI and standard API clients work great for a single project. But when you manage 30+ projects across an organization, you need a way to:

- **Query all projects at once** -- list configurations, jobs, or data lineage across the entire organization in one command
- **Give AI agents access to Keboola** -- every command supports `--json` output that agents can parse reliably
- **Onboard an entire organization** -- register all projects from a Keboola org with a single `org setup` command
- **Trace data flow** -- see how data moves between projects via bucket sharing (lineage)
- **Use MCP tools** -- call keboola-mcp-server tools across all projects in parallel
- **Manage dev branches** -- create, switch, delete branches with persistent active branch state

## What it can do

| Command group | What it does |
|---------------|-------------|
| `project` | Add, remove, edit, list, and check status of connected Keboola projects |
| `config` | Browse configurations (extractors, writers, transformations, applications) across projects |
| `job` | Browse job history -- list and inspect jobs from the Queue API |
| `lineage show` | Analyze cross-project data flow via bucket sharing (parallel across all projects) |
| `org setup` | Bulk-onboard all projects from a Keboola organization (uses Manage API) |
| `tool` | List and call MCP tools from keboola-mcp-server (read tools run across all projects in parallel) |
| `branch` | Full branch lifecycle -- create, switch, reset, delete dev branches; get merge URL |
| `explorer` | Generate KBC Explorer dashboard with catalog, orchestrations, and lineage visualization |
| `llm export` | AI-optimized project export via `kbc` Go binary (auto-resolves credentials) |
| `version` | Show kbagent version and check for kbc / MCP server updates |
| `context` | Print comprehensive usage instructions for AI agents |
| `init` | Initialize a local `.kbagent/` workspace for per-directory project isolation |
| `doctor` | Health check -- verifies config, permissions, connectivity, MCP server availability |

Every command supports `--json` for structured output and Rich formatting for human-readable output.

Run `kbagent --help` or `kbagent <command> --help` for details on any command.

## Installation

```bash
# Run directly without installing (recommended for trying it out)
uv run kbagent --help

# Install globally
uv tool install .

# Or install in development mode
uv pip install -e ".[dev]"
```

After global install, `kbagent` is available directly. Otherwise use `uv run kbagent`.

## Configuration and credentials

### Where is everything stored?

All configuration lives in a single file:

```
~/.config/keboola-agent-cli/config.json    (permissions: 0600)
```

This file contains **Storage API tokens** for each connected project. File permissions are set to `0600` (owner read/write only) to protect these tokens. Tokens are always masked in CLI output (e.g. `901-...pt0k`).

### Workspace isolation (per-client directories)

By default kbagent uses a single global config. For teams working with multiple clients, you can create **per-directory workspaces** so each client folder has its own isolated set of Keboola projects.

```bash
# Create a workspace for ACME client
mkdir -p ~/clients/acme && cd ~/clients/acme
kbagent init                    # creates .kbagent/config.json
kbagent project add --alias acme-prod --url https://connection.keboola.com --token ...

# Create a workspace for BigCorp client
mkdir -p ~/clients/bigcorp && cd ~/clients/bigcorp
kbagent init                    # separate .kbagent/config.json
kbagent project add --alias bigcorp-main --url https://connection.eu-central-1.keboola.com --token ...
```

Now when you `cd ~/clients/acme` and run `kbagent project list`, you only see ACME's projects. Each client directory can have its own `CLAUDE.md` with project-specific instructions.

**Config resolution order** (first match wins):
1. `--config-dir` CLI flag
2. `KBAGENT_CONFIG_DIR` environment variable
3. `.kbagent/config.json` in current or parent directory (walks up, like git)
4. `~/.config/keboola-agent-cli/config.json` (global default)

Run `kbagent doctor` to see which config is active. Use `kbagent init --from-global` to copy existing projects to a local workspace.

The config file structure:

```json
{
  "version": 1,
  "default_project": "prod",
  "max_parallel_workers": 10,
  "projects": {
    "prod": {
      "stack_url": "https://connection.keboola.com",
      "token": "901-xxxxx-xxxxxxx",
      "project_name": "Production",
      "project_id": 1234
    }
  }
}
```

### Manage API token is never stored

The `org setup` command requires a Manage API token to list organization projects and create Storage API tokens. This token is **never persisted** to disk -- it is either read from the `KBC_MANAGE_API_TOKEN` environment variable or prompted interactively (hidden input). It is never passed as a CLI argument and never logged.

### Parallel execution

All multi-project read operations (`config list`, `job list`, `project status`, `lineage show`, `tool call` for read tools) run in parallel. The concurrency is configurable:

**HTTP operations** (config, job, lineage, project status):

| Priority | Method | Example |
|----------|--------|---------|
| 1 (highest) | Environment variable | `KBAGENT_MAX_PARALLEL_WORKERS=20 kbagent lineage show` |
| 2 | Config file | `"max_parallel_workers": 20` in config.json |
| 3 (default) | Built-in default | `10` |

**MCP tool calls** (`tool call`, `tool list`):

By default, all MCP sessions run fully in parallel (one subprocess per project). If you hit OS resource limits on machines with many projects (50+), throttle with:

```bash
KBAGENT_MCP_MAX_SESSIONS=10 kbagent tool call get_buckets
```

## Development branches

Work on Keboola dev branches without passing `--branch` to every command:

```bash
# Create a branch -- auto-activates it for the project
kbagent branch create --project prod --name "fix-transform-x"

# All subsequent tool calls use the active branch automatically
kbagent tool call list_configs --project prod
kbagent tool call update_sql_transformation --project prod --input '{...}'

# When done, get the merge URL (opens KBC UI for safe review)
kbagent branch merge --project prod
```

The active branch is stored per-project in `config.json` and displayed in `project list` and `branch list` output. Use `branch reset` to switch back to main, or `branch delete` to remove a branch (auto-resets if it was active).

## JSON output format

All commands with `--json` return a consistent structure:

```json
{"status": "ok", "data": { ... }}
{"status": "error", "error": {"code": "INVALID_TOKEN", "message": "...", "retryable": false}}
```

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Usage error (invalid arguments) |
| 3 | Authentication error (invalid/expired token) |
| 4 | Network error (timeout, unreachable) |
| 5 | Configuration error |

## Supported Keboola stacks

Works with any Keboola stack -- AWS, Azure, GCP. Examples:

- `https://connection.keboola.com` (US, AWS)
- `https://connection.north-europe.azure.keboola.com` (Azure)
- `https://connection.europe-west3.gcp.keboola.com` (GCP)
- `https://connection.us-east4.gcp.keboola.com` (GCP)

## KBC Explorer

A standalone HTML viewer for interactive visualization of cross-project data lineage graphs. Located in `kbc-explorer/` -- see [kbc-explorer/README.md](kbc-explorer/README.md) for details.

## Development

```bash
uv pip install -e ".[dev]"
uv run pytest tests/ -v
uv run kbagent --help
```

A `Makefile` provides shortcuts for common development tasks:

```bash
make help           # show all available targets
make install        # install in dev mode
make test           # run all tests
make test-unit      # run unit tests only
make lint           # run ruff linter
make format         # format code with ruff
make check          # run lint + format-check + test (CI-like)
make clean          # remove caches and build artifacts
```

## Contributors

- [Jordan Burger](https://github.com/jordanrburger)
- [František Řehoř](https://github.com/frantisekrehor)

## License

MIT
