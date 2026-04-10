# kbagent Configuration Guide

kbagent uses a two-tier configuration system: a **global config** shared across
all directories, and optional **local workspaces** for project-specific
isolation. This guide explains how they work, when to use each, and how
resolution works.

## Architecture Overview

| Tier | Location | Created by | Scope |
|------|----------|------------|-------|
| Global | `~/.config/keboola-agent-cli/config.json` | `kbagent project add`, `kbagent org setup` | All directories on the machine |
| Local workspace | `.kbagent/config.json` (relative to project dir) | `kbagent init` | Only the directory tree containing `.kbagent/` |

The config file is a JSON document storing registered projects, the default
project alias, active branch state, and an optional permission policy. Tokens
are stored in plain text, so both tiers enforce strict file permissions.

## Resolution Priority

When kbagent starts, it resolves the config directory using this priority chain
(first match wins):

| Priority | Source | How to set |
|----------|--------|------------|
| 1 | `--config-dir` CLI flag | `kbagent --config-dir /path project list` |
| 2 | `KBAGENT_CONFIG_DIR` env var | `export KBAGENT_CONFIG_DIR=/tmp/ci-config` |
| 3 | Walk-up from CWD | Automatic -- searches parent directories for `.kbagent/config.json` |
| 4 | Global default | Automatic -- `~/.config/keboola-agent-cli/` |

The walk-up search (priority 3) works like git searching for `.git/`: kbagent
starts at your current working directory and checks each parent directory up to
`$HOME` for a `.kbagent/config.json` file. This means you can `cd` into any
subdirectory of a workspace and kbagent will still find the local config.

Run `kbagent doctor` to see which config source is active and its path.

## Global Config

The global config is the default. It lives at the platform config directory
(`~/.config/keboola-agent-cli/` on Linux/macOS) and is shared across all
directories on the machine.

**Created by:**

```bash
# Register projects individually
kbagent project add --project my-project --url https://connection.keboola.com --token <token>

# Or register all projects in an organization at once
kbagent org setup --org-id 123 --url https://connection.keboola.com
```

**Good for:**

- General use and exploration
- Multi-project workflows (lineage, sharing, cross-project queries)
- Quick one-off commands from any directory

**Security:** the config file is created with `0600` permissions (owner read/write
only). The config directory itself is created with `0700`. A `.gitignore`
containing `*` is placed inside the config directory as a defense-in-depth
measure.

## Local Workspace

A local workspace is an isolated `.kbagent/` directory in your project folder.
When present, it **shadows the global config** -- projects registered globally
will not be visible from that directory.

### When to use

- **Project isolation**: keep one project's config separate from others
- **CI/CD pipelines**: create a temporary config without touching global state
- **AI agent sandboxing**: restrict what an agent can do with `--read-only`
- **Team collaboration**: each developer initializes their own workspace in a
  shared repo (`.kbagent/` is auto-gitignored)

### Creating a local workspace

```bash
# Create an empty workspace (no projects)
kbagent init

# Copy all projects from the global config
kbagent init --from-global

# Create a read-only workspace (blocks write operations)
kbagent init --from-global --read-only
```

**What `kbagent init` does:**

1. Creates `.kbagent/config.json` in the current directory
2. Adds `.kbagent/` to `.gitignore` (creates the file if needed)
3. If `--from-global`: copies all projects and the default project setting
4. If `--read-only`: sets a permission policy that blocks `cli:write` and
   `tool:write` operations, sets the config file to `0400` (read-only), and
   creates `.claude/settings.json` rules to prevent AI agents from tampering
   with the config

**Important**: once a local workspace exists, global projects are invisible from
that directory tree. If you need projects from global, use `--from-global` when
initializing.

## Common Workflows

### Getting started (simplest path)

```bash
# Register all projects in your organization
kbagent org setup --org-id 123 --url https://connection.keboola.com

# Verify
kbagent project list
```

This creates the global config and registers every project in the organization.
Works from any directory.

### Project-specific workspace

```bash
cd my-project/

# Initialize with projects from global
kbagent init --from-global

# Local workspace is now active
kbagent project list  # same projects as global
```

Useful when you want to set a different default project or active branch without
affecting your global state.

### AI agent sandbox (read-only)

```bash
cd agent-workspace/

# Create a locked-down workspace
kbagent init --from-global --read-only

# Agent can read configs, list tables, browse jobs...
kbagent config list
kbagent storage tables --project my-project

# ...but write operations are blocked
kbagent branch create --project my-project --name "test"  # denied
```

The `--read-only` flag sets a permission policy that blocks all write CLI
commands and MCP tools. The config file itself is set to read-only (`0400`), and
Claude Code settings rules are created to prevent the agent from modifying or
bypassing the policy.

### CI/CD pipeline

```bash
# Use a temporary config directory (no local workspace, no global pollution)
export KBAGENT_CONFIG_DIR=/tmp/kbagent-ci-$$

kbagent project add --project prod \
  --url "$KBC_URL" \
  --token "$KBC_TOKEN"

kbagent config list
kbagent job list --limit 10
```

The environment variable takes priority over walk-up and global config, so the
pipeline runs in full isolation.

## Troubleshooting

### "No projects configured" after `kbagent init`

You created a local workspace without copying projects. Options:

- Delete the workspace to fall back to global: `rm -rf .kbagent/`
- Or re-initialize with projects: `rm -rf .kbagent/ && kbagent init --from-global`

### Which config am I using?

```bash
kbagent doctor
```

The first check ("Config source") shows the active config path and whether it is
`global`, `local`, `env-var`, or `cli-flag`.

### How to switch back to global

Delete the `.kbagent/` directory in the current directory (or any parent directory
that contains one):

```bash
rm -rf .kbagent/
kbagent doctor  # should now show "global"
```

### Config file has wrong permissions

```bash
kbagent doctor
```

If the doctor reports a permission warning, fix it:

```bash
chmod 600 ~/.config/keboola-agent-cli/config.json
```

## Config File Security

- **File permissions**: config files are created with `0600` (owner read/write
  only). The `kbagent doctor` command warns if permissions have drifted.
- **Tokens in plain text**: API tokens are stored as-is in the JSON file. Protect
  your config directory accordingly. Do not share or commit config files.
- **Never committed to git**: `.kbagent/` is auto-added to `.gitignore` by
  `kbagent init`. The global config directory also contains its own `.gitignore`
  with a wildcard `*` rule.
- **Manage tokens are never persisted**: the Manage API token (used by
  `kbagent org setup`) is only accepted via the `KBC_MANAGE_API_TOKEN` environment
  variable or an interactive hidden prompt. It is never written to disk.
- **Token masking in output**: tokens are never displayed in full in CLI output.
  The `mask_token()` utility shows only the first and last few characters.

## Config File Format

For reference, the config file structure:

```json
{
  "version": 1,
  "default_project": "my-project",
  "projects": {
    "my-project": {
      "stack_url": "https://connection.keboola.com",
      "token": "<storage-api-token>",
      "project_name": "My Project",
      "project_id": 12345,
      "active_branch_id": null
    }
  },
  "permissions": null
}
```

- `version`: config schema version (currently `1`)
- `default_project`: alias used when `--project` is not specified
- `projects`: map of alias to project connection details
- `permissions`: optional permission policy (set by `--read-only` or
  `kbagent permissions set`)
