# Keboola Agent CLI - Code Review & Hardening Plan

## Context

CLI tool `kbagent` for Claude Code to manage Keboola projects — heading to production.
Three parallel analyses (architecture, security, anti-patterns) produced this consolidated plan.

---

<!-- PHASE:1 -->
## Phase 1: Security Hardening

### Branch
`phase-1-security-hardening`

### Scope
Fix all critical and medium security vulnerabilities found during review:
- S1: Token exposed via `--token` CLI argument (visible in `ps aux`)
- S2: No URL validation — SSRF + protocol abuse (arbitrary URLs, plain HTTP)
- S3: Config directory created with permissive umask (0o755 instead of 0o700)
- S4: API error messages reflect arbitrary server content (Rich markup injection)
- S5: Path traversal in API URL construction (unencoded user input in paths)
- S6: `assert` in production code (stripped by `python -O`)

### Files to Create/Modify
- `src/keboola_agent_cli/commands/project.py` — Remove `--token` CLI option, add `typer.prompt(hide_input=True)` for interactive input (same pattern as manage token in `commands/org.py:31-58`). Keep `KBC_TOKEN` env var support.
- `src/keboola_agent_cli/models.py` — Add `@field_validator('stack_url')` on `ProjectConfig` enforcing `https://` scheme
- `src/keboola_agent_cli/config_store.py` — Change `mkdir()` to `mode=0o700`
- `src/keboola_agent_cli/client.py` — Truncate `api_message` to 500 chars in `_raise_api_error()`. Apply `urllib.parse.quote(value, safe="")` for `component_id`, `config_id`, `job_id` in URL paths.
- `src/keboola_agent_cli/manage_client.py` — Truncate `api_message` to 500 chars in `_raise_api_error()`
- `src/keboola_agent_cli/services/project_service.py` — Replace `assert updated is not None` (line 125) with `if updated is None: raise ConfigError(...)`
- `tests/test_cli.py` — Update `project add`/`project edit` tests for new token input flow
- `tests/test_client.py` — Add test for message truncation and URL encoding
- `tests/test_config_store.py` — Add test verifying directory permission 0o700
- `tests/test_models.py` — Add test for URL validation (reject `http://`, `file://`, accept `https://`)

### Acceptance Criteria
- [ ] `--token` is NOT accepted as CLI argument for `project add` and `project edit`
- [ ] Token is accepted via `KBC_TOKEN` env var or interactive hidden prompt
- [ ] `http://` and `file://` URLs are rejected at `project add` time with clear error
- [ ] `https://` URLs are accepted
- [ ] Config directory is created with `0o700` permissions
- [ ] API error messages are truncated to 500 characters
- [ ] Rich markup brackets `[`/`]` in error messages are escaped or stripped
- [ ] `component_id`, `config_id`, `job_id` are URL-encoded in API paths
- [ ] No `assert` statements remain in production code (only in tests)

### Tests Required
- `test_project_add_token_from_env` — token from `KBC_TOKEN` env var works
- `test_project_add_token_interactive` — interactive hidden prompt works
- `test_project_add_rejects_http_url` — `http://` URL rejected with error
- `test_project_add_rejects_file_url` — `file://` URL rejected with error
- `test_project_add_accepts_https_url` — `https://` URL accepted
- `test_config_dir_permissions` — directory created with 0o700
- `test_api_error_message_truncation` — long server response truncated to 500 chars
- `test_url_path_encoding` — special characters in component_id/config_id encoded
<!-- /PHASE:1 -->

---

<!-- PHASE:2 -->
## Phase 2: Foundation — Shared Constants & HTTP Base

### Branch
`phase-2-foundation-constants-http`

### Scope
Extract duplicated constants and HTTP retry logic into shared modules:
- D1: `_do_request()` + `_raise_api_error()` duplicated in `client.py` and `manage_client.py` (~140 lines)
- D3: Retry constants, timeouts, env var names scattered across 7+ files

### Files to Create/Modify
- `src/keboola_agent_cli/constants.py` — **NEW** — All shared constants:
  - `RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}`
  - `MAX_RETRIES = 3`, `BACKOFF_BASE = 1.0`
  - `DEFAULT_TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)`
  - `DEFAULT_STACK_URL = "https://connection.keboola.com"`
  - `DEFAULT_TOKEN_DESCRIPTION = "kbagent-cli"`
  - `DEFAULT_JOB_LIMIT = 50`, `MAX_JOB_LIMIT = 500`
  - `MAX_API_ERROR_LENGTH = 500`
  - Env var names: `ENV_MAX_PARALLEL_WORKERS`, `ENV_KBC_TOKEN`, `ENV_KBC_STORAGE_API_URL`, `ENV_KBC_MANAGE_API_TOKEN`
- `src/keboola_agent_cli/http_base.py` — **NEW** — `BaseHttpClient` with shared `_do_request()`, `_raise_api_error()`, retry logic, message sanitization. Imports constants from `constants.py`.
- `src/keboola_agent_cli/client.py` — Inherit from `BaseHttpClient`, remove duplicated methods
- `src/keboola_agent_cli/manage_client.py` — Inherit from `BaseHttpClient`, remove duplicated methods
- `src/keboola_agent_cli/services/base.py` — Import `ENV_MAX_PARALLEL_WORKERS` from `constants.py`
- `src/keboola_agent_cli/commands/project.py` — Import `DEFAULT_STACK_URL` from `constants.py`
- `src/keboola_agent_cli/commands/job.py` — Import `DEFAULT_JOB_LIMIT`, `MAX_JOB_LIMIT` from `constants.py`
- `src/keboola_agent_cli/services/job_service.py` — Import `DEFAULT_JOB_LIMIT` from `constants.py`
- `src/keboola_agent_cli/services/org_service.py` — Import `DEFAULT_TOKEN_DESCRIPTION` from `constants.py`
- `src/keboola_agent_cli/commands/org.py` — Import `DEFAULT_TOKEN_DESCRIPTION`, `ENV_KBC_MANAGE_API_TOKEN` from `constants.py`
- `tests/test_client.py` — Update to work with `BaseHttpClient` inheritance
- `tests/test_manage_client.py` — Update to work with `BaseHttpClient` inheritance

### Acceptance Criteria
- [ ] No duplicate `RETRYABLE_STATUS_CODES`, `MAX_RETRIES`, `BACKOFF_BASE` definitions remain
- [ ] No duplicate `_do_request()` or `_raise_api_error()` methods remain
- [ ] No hardcoded timeout values in `client.py` or `manage_client.py` — all from `constants.py`
- [ ] `BaseHttpClient` in `http_base.py` contains all shared HTTP logic
- [ ] `KeboolaClient` and `ManageClient` both inherit from `BaseHttpClient`
- [ ] All existing tests pass without modification (behavior unchanged)
- [ ] No hardcoded `"kbagent-cli"` strings remain outside `constants.py`
- [ ] No hardcoded `50` / `500` job limits remain outside `constants.py`

### Tests Required
- All existing tests in `test_client.py` and `test_manage_client.py` must still pass
- `test_base_http_client_retry` — verify retry logic works via base class
- `test_base_http_client_error_sanitization` — verify message truncation in base class
<!-- /PHASE:2 -->

---

<!-- PHASE:3 -->
## Phase 3: Command Layer Deduplication

### Branch
`phase-3-command-helpers`

### Scope
Extract duplicated command-layer patterns:
- D2: `_get_formatter()`/`_get_service()` duplicated in all 7 command files
- A4: Exit code mapping duplicated in 4 command files with inconsistent logic
- Error warning loop duplicated in 4 command files

### Files to Create/Modify
- `src/keboola_agent_cli/commands/_helpers.py` — **NEW** — Shared command helpers:
  - `get_formatter(ctx: typer.Context) -> OutputFormatter`
  - `get_service(ctx: typer.Context, key: str) -> Any`
  - `map_error_to_exit_code(exc: KeboolaApiError) -> int` (unified 3-case: INVALID_TOKEN->3, TIMEOUT/CONNECTION_ERROR/RETRY_EXHAUSTED->4, else->1)
  - `emit_project_warnings(formatter: OutputFormatter, result: dict) -> None`
- `src/keboola_agent_cli/commands/project.py` — Remove `_get_formatter`, `_get_service`, use `_helpers`. Fix 2-case exit mapping to 3-case.
- `src/keboola_agent_cli/commands/config.py` — Remove `_get_formatter`, `_get_service`, exit code block, warning loop. Use `_helpers`.
- `src/keboola_agent_cli/commands/job.py` — Remove `_get_formatter`, `_get_service`, exit code block, warning loop. Use `_helpers`.
- `src/keboola_agent_cli/commands/lineage.py` — Remove `_get_formatter`, `_get_service`, warning loop. Use `_helpers`.
- `src/keboola_agent_cli/commands/org.py` — Remove `_get_formatter`, `_get_service`, exit code block. Use `_helpers`.
- `src/keboola_agent_cli/commands/tool.py` — Remove `_get_formatter`, `_get_service`, warning loop. Use `_helpers`.
- `src/keboola_agent_cli/commands/context.py` — Remove `_get_formatter`. Use `_helpers`.
- `tests/test_cli.py` — Verify all commands still work correctly

### Acceptance Criteria
- [ ] No `_get_formatter` / `_get_service` functions remain in individual command files
- [ ] All command files import from `commands._helpers`
- [ ] Exit code mapping is consistent across all commands (3-case logic)
- [ ] Error warning loop is not duplicated — all commands use `emit_project_warnings`
- [ ] All existing CLI tests pass

### Tests Required
- `test_map_error_to_exit_code_invalid_token` — returns 3
- `test_map_error_to_exit_code_timeout` — returns 4
- `test_map_error_to_exit_code_connection` — returns 4
- `test_map_error_to_exit_code_other` — returns 1
- All existing `test_cli.py` tests must still pass
<!-- /PHASE:3 -->

---

<!-- PHASE:4 -->
## Phase 4: Architecture Cleanup

### Branch
`phase-4-architecture-cleanup`

### Scope
Fix architectural issues:
- A1: `doctor.py` (294 lines) contains service-layer logic — violates 3-layer design
- A3: `except (KeboolaApiError, Exception)` — redundant and confusing
- A5: Async tasks collected sequentially instead of `asyncio.gather`
- D4: `LineageService` reimplements `_run_parallel` instead of using `BaseService`
- D5: `_gather_read_results` and `_gather_auto_expand_results` are near-identical

### Files to Create/Modify
- `src/keboola_agent_cli/services/doctor_service.py` — **NEW** — Extract `_check_config_file`, `_check_config_valid`, `_check_connectivity`, `_check_version` from `commands/doctor.py`
- `src/keboola_agent_cli/commands/doctor.py` — Thin command that calls `DoctorService`. Use `_helpers` from Phase 3.
- `src/keboola_agent_cli/cli.py` — Wire `DoctorService` into `ctx.obj`
- `src/keboola_agent_cli/services/org_service.py` — Split `except (KeboolaApiError, Exception)` (line 155) into separate blocks
- `src/keboola_agent_cli/services/mcp_service.py` — Replace sequential `for a, task in tasks.items(): await task` with `asyncio.gather(*tasks.values(), return_exceptions=True)`. Extract shared gather helper from `_gather_read_results` and `_gather_auto_expand_results`.
- `src/keboola_agent_cli/services/lineage_service.py` — Refactor to use `BaseService._run_parallel()` instead of own `ThreadPoolExecutor`
- `tests/test_doctor_service.py` — **NEW** — Unit tests for `DoctorService`
- `tests/test_cli.py` — Update doctor tests
- `tests/test_mcp_service.py` — Update for asyncio.gather changes
- `tests/test_lineage_service.py` — Update for `_run_parallel` usage

### Acceptance Criteria
- [ ] `commands/doctor.py` is a thin command (<50 lines) with no business logic
- [ ] `DoctorService` contains all check logic and is testable independently
- [ ] No `except (KeboolaApiError, Exception)` remains — split into two separate blocks
- [ ] MCP service uses `asyncio.gather` for concurrent task collection
- [ ] `LineageService` uses `BaseService._run_parallel()` — no custom `ThreadPoolExecutor`
- [ ] `_gather_read_results` and `_gather_auto_expand_results` share common gather helper
- [ ] `kbagent doctor` works correctly end-to-end

### Tests Required
- `test_doctor_service_check_config_file` — config file existence check
- `test_doctor_service_check_config_valid` — config validation check
- `test_doctor_service_check_connectivity` — API connectivity check
- `test_mcp_gather_with_asyncio_gather` — concurrent task collection
- `test_lineage_uses_run_parallel` — lineage uses base service parallel
- All existing doctor, MCP, and lineage tests must still pass
<!-- /PHASE:4 -->

---

<!-- PHASE:5 -->
## Phase 5: Production Readiness

### Branch
`phase-5-production-readiness`

### Scope
Add observability and resilience features:
- P1: Add structured logging throughout (zero `import logging` in src/ currently)
- P2: Make MCP timeouts configurable via env vars
- P3: Cap `max_parallel_workers` at 100
- P4: Respect `Retry-After` header on 429 responses
- P5: Wire up unused `--verbose` flag to logging
- P6: Warn on non-standard queue URL derivation

### Files to Create/Modify
- `src/keboola_agent_cli/cli.py` — Configure root logger in callback: `--verbose` -> DEBUG, default -> WARNING. Wire `verbose` to logging level.
- `src/keboola_agent_cli/http_base.py` — Add `logger.debug()` for retry attempts. Read `Retry-After` header on 429 (capped at 60s).
- `src/keboola_agent_cli/client.py` — Add `logger.warning()` when queue URL derivation doesn't change hostname
- `src/keboola_agent_cli/config_store.py` — Add `logger.debug()` for config load/save operations
- `src/keboola_agent_cli/services/base.py` — Add `logger.debug()` for worker errors in `_run_parallel`
- `src/keboola_agent_cli/services/mcp_service.py` — Add `logger.info()` for MCP server lifecycle. Read `KBAGENT_MCP_TOOL_TIMEOUT` and `KBAGENT_MCP_INIT_TIMEOUT` env vars.
- `src/keboola_agent_cli/services/org_service.py` — Add `logger.info()` for token creation operations
- `src/keboola_agent_cli/constants.py` — Add `ENV_MCP_TOOL_TIMEOUT`, `ENV_MCP_INIT_TIMEOUT`, `DEFAULT_MCP_TOOL_TIMEOUT = 60`, `DEFAULT_MCP_INIT_TIMEOUT = 30`, `MAX_PARALLEL_WORKERS_LIMIT = 100`, `MAX_RETRY_AFTER_SECONDS = 60`
- `src/keboola_agent_cli/models.py` — Add `le=100` to `max_parallel_workers` Field
- `tests/test_cli.py` — Test `--verbose` enables debug logging
- `tests/test_client.py` — Test `Retry-After` header is respected

### Acceptance Criteria
- [ ] `--verbose` flag produces DEBUG-level log output to stderr
- [ ] Default log level is WARNING (no noise for normal usage)
- [ ] HTTP retries are logged with attempt number and delay
- [ ] MCP server start/stop is logged
- [ ] MCP timeouts are configurable via `KBAGENT_MCP_TOOL_TIMEOUT` and `KBAGENT_MCP_INIT_TIMEOUT`
- [ ] `max_parallel_workers` rejects values > 100 with Pydantic validation error
- [ ] 429 responses use `Retry-After` header value (capped at 60s) instead of fixed backoff
- [ ] Non-standard queue URL derivation produces a warning log

### Tests Required
- `test_verbose_enables_debug_logging` — `--verbose` sets DEBUG level
- `test_retry_after_header_respected` — 429 with Retry-After uses that value
- `test_retry_after_capped` — Retry-After > 60s is capped
- `test_max_workers_upper_bound` — `max_parallel_workers=200` raises ValidationError
- `test_mcp_timeout_from_env` — env var overrides default timeout
<!-- /PHASE:5 -->

---

<!-- PHASE:6 -->
## Phase 6: Model Validation & Dead Code Cleanup

### Branch
`phase-6-models-cleanup`

### Scope
Tighten model validation and remove dead code:
- M2: `TokenVerifyResponse` fields have silent defaults — malformed responses pass silently
- M3: `project_id` default `0` is falsy — masks parsing failures
- M4: MCP tool name not validated against known tool list
- C1-C4: Dead code (redundant imports, unnecessary override, duplicate IDs)

### Files to Create/Modify
- `src/keboola_agent_cli/models.py` — Remove `default=""` and `default=0` from `TokenVerifyResponse` required fields. Change `project_id: int = Field(default=0)` to `project_id: int | None = Field(default=None)`.
- `src/keboola_agent_cli/services/mcp_service.py` — Validate `tool_name` exists in tool list before calling (return clear error instead of `return []`). Remove unnecessary `McpService.__init__` override (line 319-324). Add deduplication to `_extract_ids`: `return list(dict.fromkeys(ids))`.
- `src/keboola_agent_cli/output.py` — Remove `import json as _json` (line 195), use top-level `json`. Move `from datetime import datetime` (line 301) to top-level imports.
- `src/keboola_agent_cli/services/lineage_service.py` — Update `project_id` checks from `== 0` to `is None`
- `tests/test_models.py` — Test `TokenVerifyResponse` rejects empty/missing required fields. Test `project_id=None` default.
- `tests/test_mcp_service.py` — Test unknown tool name returns clear error. Test `_extract_ids` deduplication.

### Acceptance Criteria
- [ ] `TokenVerifyResponse` with missing `owner_name` raises `ValidationError`
- [ ] `project_id` defaults to `None` not `0`
- [ ] All code checking `project_id` handles `None` correctly
- [ ] Unknown MCP tool name returns clear error message (not empty list)
- [ ] `_extract_ids` returns unique values (no duplicates)
- [ ] No `import json as _json` inside functions — uses top-level import
- [ ] No `from datetime import datetime` inside functions — uses top-level import
- [ ] No unnecessary `__init__` override in `McpService`

### Tests Required
- `test_token_verify_response_rejects_missing_fields` — missing required fields raise error
- `test_project_id_default_none` — default is None not 0
- `test_unknown_tool_name_error` — clear error for nonexistent tool
- `test_extract_ids_deduplication` — duplicates removed, order preserved
- All existing tests must still pass
<!-- /PHASE:6 -->

---

<!-- PHASE:7 -->
## Phase 7: Test Coverage

### Branch
`phase-7-test-coverage`

### Scope
Fill testing gaps identified during review:
- T1: No CLI-level tests for `lineage` command
- T2: No CLI-level tests for `org setup` command
- T3: `_resolve_manage_token()` in `org.py` untested
- T4: `output.py` format functions not directly unit-tested
- T5: `_extract_ids()` not directly unit-tested (if not covered in Phase 6)
- T6: Test helpers duplicated across test files

### Files to Create/Modify
- `tests/conftest.py` — Move `_make_mock_client`, `_setup_single_project`, `_setup_two_projects` from `test_cli.py`, `test_services.py`, `test_base_service.py`, `test_lineage_service.py` into shared fixtures
- `tests/test_cli.py` — Add `TestLineageShow` (test `kbagent lineage` and `kbagent lineage show`), `TestOrgSetup` (test interactive token, env var token, dry-run, confirmation), `TestResolveManageToken` (env var, TTY prompt, non-TTY error). Remove inlined helpers now in conftest.
- `tests/test_output.py` — Add tests for `format_lineage_table`, `format_tool_result`, `format_tools_table`, `format_job_detail`, `format_config_detail`, `_format_duration`, `_seconds_to_human`
- `tests/test_services.py` — Remove inlined helpers now in conftest
- `tests/test_base_service.py` — Remove inlined helpers now in conftest
- `tests/test_lineage_service.py` — Remove inlined helpers now in conftest

### Acceptance Criteria
- [ ] `kbagent lineage` has CLI-level tests (JSON and human output)
- [ ] `kbagent org setup` has CLI-level tests (dry-run, confirmation, token sources)
- [ ] `_resolve_manage_token()` is tested for all 3 paths (env, TTY, non-TTY)
- [ ] All `output.py` format functions have direct unit tests
- [ ] No duplicate test helpers — all shared fixtures in `conftest.py`
- [ ] All tests pass

### Tests Required
- `TestLineageShow::test_lineage_show_json` — JSON output format
- `TestLineageShow::test_lineage_show_human` — Rich table output
- `TestLineageShow::test_lineage_default_subcommand` — `kbagent lineage` invokes `show`
- `TestOrgSetup::test_org_setup_dry_run` — dry run mode
- `TestOrgSetup::test_org_setup_with_env_token` — token from env var
- `TestOrgSetup::test_org_setup_confirmation_declined` — user declines
- `TestResolveManageToken::test_token_from_env` — env var path
- `TestResolveManageToken::test_token_from_prompt` — interactive prompt
- `TestResolveManageToken::test_non_tty_error` — non-TTY error
- `test_format_lineage_table` — lineage table rendering
- `test_format_duration` — duration formatting
- `test_seconds_to_human` — seconds to human-readable
<!-- /PHASE:7 -->

---

## Summary

| Phase | Branch | Tasks | Scope |
|-------|--------|-------|-------|
| 1 | `phase-1-security-hardening` | 6 security fixes | ~10 files |
| 2 | `phase-2-foundation-constants-http` | Constants + HTTP base | 2 new + ~10 modified |
| 3 | `phase-3-command-helpers` | Command dedup | 1 new + ~8 modified |
| 4 | `phase-4-architecture-cleanup` | Doctor, async, lineage | 1 new + ~6 modified |
| 5 | `phase-5-production-readiness` | Logging, timeouts, resilience | ~10 modified |
| 6 | `phase-6-models-cleanup` | Validation + dead code | ~6 modified |
| 7 | `phase-7-test-coverage` | Test gaps + shared fixtures | ~6 modified |
