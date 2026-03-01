# Plan: Parallelize lineage show across projects

## Context

`kbagent lineage show` queries each project sequentially in a `for` loop (`lineage_service.py:98`). With 34 projects, this means 34 serial HTTP requests to `list_buckets`, making the command very slow. The fix is to parallelize these API calls.

## Approach: `concurrent.futures.ThreadPoolExecutor`

Using `ThreadPoolExecutor` instead of `asyncio` because:
- `KeboolaClient` uses sync `httpx.Client` — no async rewrite needed
- Minimal code change (only `lineage_service.py`)
- Each thread gets its own `KeboolaClient` instance (already the case — created per project)
- I/O-bound workload = threads are perfectly fine

## Changes

### File: `src/keboola_agent_cli/services/lineage_service.py`

1. Add import: `from concurrent.futures import ThreadPoolExecutor, as_completed`
2. Extract per-project work into a helper method `_fetch_project_buckets()` that:
   - Creates a client
   - Calls `list_buckets(include="linkedBuckets")`
   - Returns `(alias, project, buckets)` on success
   - Returns `(alias, error_dict)` on failure
   - Always closes the client in `finally`
3. In `get_lineage()`, replace the sequential `for` loop with:
   - `ThreadPoolExecutor(max_workers=min(len(projects), 10))` — cap at 10 to avoid overwhelming the API
   - Submit all projects, collect results via `as_completed`
   - Process buckets from each result sequentially (bucket processing is CPU-bound, fast)

### File: `tests/test_lineage_service.py`

4. Add a test verifying parallel execution (mock clients, assert all are called)

## Verification

```bash
# Run existing tests to ensure no regressions
uv run pytest tests/test_lineage_service.py -v

# Manual test with real projects
uv run kbagent lineage show
```
