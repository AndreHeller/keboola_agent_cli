# Keboola Agent CLI (`kbagent`)

A CLI for managing multiple Keboola projects from one place. Built for AI coding agents (Claude Code, Codex, Gemini) and human operators who need to work across many projects at once.

## Why does this exist?

Keboola's web UI and standard API clients work great for a single project. But when you manage 30+ projects across an organization, you need a way to:

- **Query all projects at once** -- list configurations, jobs, or data lineage across the entire organization in one command
- **Give AI agents access to Keboola** -- every command supports `--json` output that agents can parse reliably
- **Onboard an entire organization** -- register all projects from a Keboola org with a single `org setup` command
- **Trace data flow** -- see how data moves between projects via bucket sharing (lineage)
- **Use MCP tools** -- call keboola-mcp-server tools across all projects in parallel

## What it can do

| Command group | What it does |
|---------------|-------------|
| `project` | Add, remove, edit, list, and check status of connected Keboola projects |
| `config` | Browse configurations (extractors, writers, transformations, applications) across projects |
| `job` | Browse job history -- list and inspect jobs from the Queue API |
| `lineage` | Analyze cross-project data flow via bucket sharing (parallel across all projects) |
| `org setup` | Bulk-onboard all projects from a Keboola organization (uses Manage API) |
| `tool` | List and call MCP tools from keboola-mcp-server (read tools run across all projects in parallel) |
| `context` | Print comprehensive usage instructions for AI agents |
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

All multi-project read operations (`config list`, `job list`, `project status`, `lineage show`, `tool call` for read tools) run in parallel using a thread pool. The concurrency is configurable:

| Priority | Method | Example |
|----------|--------|---------|
| 1 (highest) | Environment variable | `KBAGENT_MAX_PARALLEL_WORKERS=20 kbagent lineage show` |
| 2 | Config file | `"max_parallel_workers": 20` in config.json |
| 3 (default) | Built-in default | `10` |

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

## Development

```bash
uv pip install -e ".[dev]"
uv run pytest tests/ -v
uv run kbagent --help
```

## License

MIT
