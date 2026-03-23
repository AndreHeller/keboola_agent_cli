---
name: kbagent
description: >
  Use when working with Keboola Connection projects via kbagent CLI.
  Covers: exploring configurations (extractors, writers, transformations),
  browsing job history, analyzing cross-project data lineage, calling MCP tools
  across multiple projects, managing development branches, debugging SQL in
  temporary workspaces, bulk-onboarding organizations, and generating explorer
  dashboards. Triggers: kbagent, Keboola project, keboola configs, keboola jobs,
  keboola lineage, keboola transformations, keboola MCP tools, keboola workspace,
  SQL debugging, keboola branches, keboola organization, keboola explorer.
---

# kbagent -- Keboola Agent CLI

## First step -- always

Load full CLI documentation before doing anything else:

```bash
kbagent context
```

This prints all commands, flags, workflows, and tips. Read it fully before proceeding.

## Rules

1. **Always use `--json`**: `kbagent --json <command>` for parseable output
2. **Multi-project by default**: read commands query ALL connected projects in parallel -- no need to loop
3. **Write commands need `--project`**: specify the target project alias
4. **Tokens are always masked** in output -- this is expected, not an error

## Choosing the right approach

| Goal | Command |
|------|---------|
| See what projects are connected | `kbagent --json project list` |
| Browse configs across projects | `kbagent --json config list` |
| Check for failed jobs | `kbagent --json job list --status error` |
| Understand data flow between projects | `kbagent --json lineage` |
| Generate visual dashboard | `kbagent explorer` |
| Call MCP tools (read, across all projects) | `kbagent --json tool call <tool>` |
| Call MCP tools (write, single project) | `kbagent --json tool call <tool> --project <alias> --input '{...}'` |
| Debug SQL from a transformation | See [workspace workflow](references/workspace-workflow.md) |
| Work on a dev branch safely | See [branch workflow](references/branch-workflow.md) |
| Onboard entire organization | `KBC_MANAGE_API_TOKEN=xxx kbagent --json org setup --org-id ID --url URL --yes` |

## Response format

All JSON responses follow one of two shapes:

**Success:**
```json
{"status": "ok", "data": ...}
```

**Error:**
```json
{"status": "error", "error": {"code": "ERROR_CODE", "message": "...", "retryable": true}}
```

Check the `retryable` field -- if `true`, retry the operation.

For detailed response parsing rules and common pitfalls, see [gotchas](references/gotchas.md).

## First-time setup

If kbagent is not yet installed:

```bash
uv tool install git+https://github.com/padak/keboola_agent_cli
uv tool install --prerelease=allow keboola-mcp-server
kbagent doctor --fix
```

Then add projects:

```bash
# Single project
kbagent --json project add --alias prod --url https://connection.keboola.com --token YOUR_TOKEN

# Or bulk-onboard from organization
KBC_MANAGE_API_TOKEN=xxx kbagent --json org setup --org-id 123 --url https://connection.keboola.com --yes
```
