# Sync Workflow -- GitOps for Keboola Configurations

Sync lets you manage Keboola configurations as local files with full git integration.

## All-projects workflow (recommended)

```bash
# Download all configured projects in one command
mkdir keboola && cd keboola
kbagent sync pull --all-projects

# Check status across all projects (compact one-liner per project)
kbagent sync diff --all-projects

# Push changes from all projects
kbagent sync push --all-projects --dry-run   # preview
kbagent sync push --all-projects             # apply
```

Each project gets its own subdirectory (named by alias). Projects are processed in parallel.

## Single-project workflow

```bash
# Pull auto-inits if no manifest exists
kbagent --json sync pull --project prod

# Edit locally -- configs are in _config.yml, description in _description.md,
# SQL in transform.sql, Python in code.py
# Use any IDE, get git diffs, code review, etc.

# Review changes
kbagent --json sync status                         # what changed locally
kbagent --json sync diff --project prod            # 3-way diff vs remote

# Push
kbagent --json sync push --project prod --dry-run  # preview
kbagent --json sync push --project prod            # apply
```

## File format

Every config directory contains:

| File | Purpose |
|------|---------|
| `_config.yml` | YAML config (name, parameters, storage) |
| `_description.md` | Description as readable Markdown (always separate) |

Depending on component type, additional files are extracted:

| Component type | Extra files |
|---------------|-------------|
| Snowflake transformation | `transform.sql` (SQL with `/* ===== BLOCK: ... ===== */` markers) |
| Python transformation | `transform.py` + `pyproject.toml` (dependencies) |
| Custom Python app | `code.py` + `pyproject.toml` |
| Flow/orchestrator | phases, tasks, schedules inline in `_config.yml` |

## Git-branching workflow (recommended)

Maps git branches to Keboola dev branches for safe parallel development.

```bash
# Initialize with git-branching
git init
kbagent --json sync init --project prod --git-branching
kbagent --json sync pull --project prod
git add -A && git commit -m "initial sync"

# Create feature branch
git checkout -b feature/new-etl
kbagent --json sync branch-link --project prod
# -> Creates Keboola dev branch "feature/new-etl"
# -> All sync commands now auto-target this dev branch

# Work on the feature branch
# Edit _config.yml, transform.sql, etc.
kbagent --json sync diff --project prod     # compares vs dev branch
kbagent --json sync push --project prod     # pushes to dev branch ONLY

# Production is NEVER touched from feature branches
# Unlinked branches are BLOCKED from sync operations
```

### Branch mapping

Stored in `.keboola/branch-mapping.json`:

```json
{
  "mappings": {
    "main": {"id": null, "name": "Main"},
    "feature/new-etl": {"id": "123456", "name": "feature/new-etl"}
  }
}
```

- `id: null` = production (default branch)
- `id: "123456"` = Keboola dev branch
- Sync commands auto-resolve the target branch from the current git branch

### Merge back to production

1. Merge in Keboola UI: `kbagent branch merge --project prod` (returns URL)
2. Git merge: `git checkout main && git merge feature/new-etl`
3. Sync merged state: `kbagent --json sync pull --project prod`
4. Cleanup: `kbagent sync branch-unlink` + delete git branch

## 3-way diff

`sync diff` uses a 3-way comparison (local vs pull-time base vs remote):

| Change type | Meaning | Action |
|------------|---------|--------|
| MODIFIED | Local changed, remote unchanged | Safe to push |
| REMOTE MODIFIED | Remote changed, local unchanged | Run pull to fetch |
| CONFLICT | Both sides changed | Resolve manually, then push |
| ADDED | New local config | Push creates it |
| DELETED | Local file removed | Push deletes from remote |

## Key behaviors

- **Pull is idempotent**: re-running pull when nothing changed writes zero files
- **Pull protects local edits**: modified files are skipped (use `--force` to overwrite)
- **Push only sends local changes**: remote_modified and conflict changes are skipped
- **Encrypted values**: nonce differences are ignored in diff (no false positives)
- **New configs**: push auto-assigns IDs from the API, updates manifest
