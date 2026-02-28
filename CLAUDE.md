# CLAUDE.md - Development Context for Claude Code

## Build and Run

```bash
# Install in development mode (editable)
uv pip install -e ".[dev]"

# Or install dependencies only
uv sync

# Run the CLI
kbagent --help
uv run kbagent --help

# Run a specific command
kbagent --json project list
```

## Testing

```bash
# Run all tests
uv run pytest tests/ -v

# Run a specific test file
uv run pytest tests/test_cli.py -v

# Run a specific test class or method
uv run pytest tests/test_cli.py::TestProjectAdd -v
uv run pytest tests/test_cli.py::TestProjectAdd::test_project_add_success_json -v
```

## Project Structure

```
src/keboola_agent_cli/
  __init__.py           # __version__ = "0.1.0"
  __main__.py           # python -m support
  cli.py                # Typer root app, global options, subcommand wiring
  client.py             # LAYER 3: HTTP client (Storage API + Queue API)
  manage_client.py      # LAYER 3: HTTP client (Manage API, X-KBC-ManageApiToken)
  config_store.py       # JSON persistence for config.json (0600 permissions)
  models.py             # Pydantic models shared across layers
  output.py             # OutputFormatter: JSON vs Rich dual-mode output
  errors.py             # KeboolaApiError, ConfigError, mask_token()
  commands/
    project.py          # LAYER 1: CLI commands for project management
    config.py           # LAYER 1: CLI commands for config browsing
    job.py              # LAYER 1: CLI commands for job history (Queue API)
    lineage.py          # LAYER 1: CLI commands for cross-project data lineage
    org.py              # LAYER 1: CLI commands for organization bulk onboarding
    tool.py             # LAYER 1: CLI commands for MCP tool list/call
    explorer.py         # LAYER 1: CLI commands for KBC Explorer dashboard generation
    context.py          # LAYER 1: Agent usage instructions
    doctor.py           # LAYER 1: Health check command
  services/
    base.py             # LAYER 2: BaseService - shared parallel execution infrastructure
    project_service.py  # LAYER 2: Business logic for projects
    config_service.py   # LAYER 2: Business logic for configurations
    job_service.py      # LAYER 2: Business logic for job history
    lineage_service.py  # LAYER 2: Cross-project lineage via bucket sharing
    org_service.py      # LAYER 2: Organization setup orchestration
    mcp_service.py      # LAYER 2: MCP tool integration (keboola-mcp-server wrapper)
    explorer_service.py # LAYER 2: KBC Explorer catalog/orchestration generation

tests/
  conftest.py           # Shared fixtures (tmp_config_dir, config_store, formatters)
  test_cli.py           # End-to-end CLI tests via CliRunner
  test_client.py        # API client tests with mocked HTTP
  test_manage_client.py # Manage API client tests with mocked HTTP
  test_config_store.py  # Config persistence tests
  test_errors.py        # mask_token() tests
  test_models.py        # Pydantic model tests
  test_output.py        # OutputFormatter tests
  test_services.py      # Business logic tests (project, config, parallel)
  test_base_service.py     # BaseService unit tests (resolve, workers, parallel)
  test_lineage_service.py  # Lineage service tests
  test_mcp_service.py      # MCP service tests
  test_org_service.py      # Org service tests (slugify, setup, idempotency)
  test_explorer_service.py # Explorer service tests (tier assignment, job stats, generation)
  test_integration.py      # Integration tests (edge cases, linting)
```

## Architecture: 3-Layer Design

```
CLI Commands (commands/)  -->  Services (services/)  -->  API Client (client.py, manage_client.py)
  Typer, output                 Business logic             HTTP, endpoints
```

- API changes: modify only `client.py` or `manage_client.py`
- Business logic changes: modify only `services/`
- UI changes: modify only `commands/`

### Two HTTP Clients

- **KeboolaClient** (`client.py`): Storage API + Queue API, auth via `X-StorageApi-Token`
- **ManageClient** (`manage_client.py`): Manage API, auth via `X-KBC-ManageApiToken`

Both share the same retry/backoff pattern (429/5xx, exponential backoff, 3 retries).

### MCP Integration

`McpService` wraps `keboola-mcp-server` as a subprocess via MCP SDK (`mcp` package).
- Read tools run across ALL projects in parallel (one MCP session per project)
- Write tools target a single project (default or `--project`)
- **Auto-expand**: tools like `list_tables` that require `bucket_id` automatically
  resolve it by calling `list_buckets` first (configured in `AUTO_EXPAND_TOOLS` dict)
- Upfront parameter validation against tool's `inputSchema` before multi-project dispatch

## Coding Conventions

1. **Typer commands** are thin - they parse arguments, call a service, and format output. No business logic in commands.

2. **Services** receive `ConfigStore` and a `client_factory` callable via dependency injection. This enables easy testing with mocks.

3. **All data models** use Pydantic 2.x (`BaseModel`). Models are defined in `models.py` and shared across layers.

4. **Dual output**: every command supports `--json` for structured output and Rich formatting for human-readable output. Use `OutputFormatter.output(data, human_formatter)`.

5. **Error handling**: commands catch `KeboolaApiError` and `ConfigError`, map them to the appropriate exit code, and output structured errors in JSON mode.

6. **Exit codes**: 0=success, 1=general error, 2=usage error, 3=auth error, 4=network error, 5=config error.

7. **Token masking**: tokens are never printed in full. Use `mask_token()` from `errors.py`.

8. **Config file**: stored at `~/.config/keboola-agent-cli/config.json` with `0600` permissions. Managed by `ConfigStore`.

9. **Tests**: use `typer.testing.CliRunner` for CLI tests, `unittest.mock` for mocking services and clients, `pytest` fixtures from `conftest.py`.

10. **Dependencies**: typer, rich, httpx, pydantic, platformdirs, mcp, jsonschema, pyyaml. Dev: pytest, pytest-httpx.

11. **Error accumulation**: multi-project operations collect per-project errors without stopping. One project failing doesn't block others (see `lineage_service.py`, `org_service.py`).

12. **Manage token security**: never persisted, never passed as CLI argument, never logged. Only via `KBC_MANAGE_API_TOKEN` env var or interactive hidden prompt.

13. **Idempotency**: `org setup` skips already-registered projects by matching `project_id`. Safe to re-run.

## All CLI Commands

```
kbagent project add --alias NAME --url URL --token TOKEN
kbagent project list
kbagent project remove --alias NAME
kbagent project edit --alias NAME [--url URL] [--token TOKEN]
kbagent project status [--project NAME]

kbagent config list [--project NAME] [--component-type TYPE] [--component-id ID]
kbagent config detail --project NAME --component-id ID --config-id ID

kbagent job list [--project NAME] [--component-id ID] [--status STATUS] [--limit N]
kbagent job detail --project NAME --job-id ID

kbagent lineage [--project NAME]

kbagent org setup --org-id ID --url URL [--dry-run] [--yes] [--token-description PREFIX]

kbagent tool list [--project NAME]
kbagent tool call TOOL_NAME [--project NAME] [--input JSON]

kbagent explorer [--project NAME] [--output-dir DIR] [--job-limit N] [--tiers FILE] [--no-open]

kbagent context
kbagent doctor
```
