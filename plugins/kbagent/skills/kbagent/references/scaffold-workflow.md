# Creating New Configurations (Scaffold Workflow)

## When to use

- User wants to create a new extractor, writer, transformation, or application
- User asks "how do I set up a new Snowflake extractor" or similar
- User wants to scaffold config files for a component

## Step-by-step workflow

### 1. Find the component

```bash
kbagent --json component list --project ALIAS --query "description of what they need"
```

Returns ranked suggestions with component_id, name, type, score.

If the user already knows the component ID, skip this step.

### 2. Inspect component (optional)

```bash
kbagent --json component detail --component-id COMPONENT_ID --project ALIAS
```

Shows schema summary, examples count, documentation URL.

### 3. Generate scaffold

```bash
# To stdout (inspect before writing)
kbagent --json config new --component-id COMPONENT_ID --project ALIAS --name "Config Name"

# To disk (auto-detects kbc project structure, writes under main/ if applicable)
kbagent config new --component-id COMPONENT_ID --project ALIAS --name "Config Name" --output-dir .
```

Generated files by component type:

| Type | Files |
|------|-------|
| Extractor/Writer | `_config.yml` |
| SQL transformation | `_config.yml` + `transform.sql` |
| Python transformation | `_config.yml` + `transform.py` + `pyproject.toml` |
| Custom Python app | `_config.yml` + `code.py` + `pyproject.toml` |
| Flow/Orchestrator | `_config.yml` (with phases/tasks/schedules) |

### 4. Edit the scaffold

- Fill in actual parameter values (hostnames, database names, etc.)
- Replace `<YOUR_SECRET>` placeholders with actual credentials
- Adjust storage input/output table mappings

### 5. Push to Keboola

```bash
kbagent sync push --project ALIAS
```

This automatically:
- Encrypts all `#`-prefixed secret fields (e.g. `#password`) via Encryption API
- Creates the configuration in Keboola and gets a config_id
- Writes back encrypted values + config_id to local `_config.yml`

## Important notes

- `_config.yml` format follows the kbc CLI dev-friendly YAML structure
- The `_keboola.component_id` field in `_config.yml` is required for push to work
- `config_id` is assigned by Keboola on first push -- don't set it manually
- Secret fields use Keboola convention: any key starting with `#` is a secret
- Scaffold marks secret placeholders with `# encrypted by Keboola on push` comments
- Encrypted values look like `KBC::ProjectSecure::...`
- Already-encrypted values are NOT re-encrypted on subsequent pushes
- If `--output-dir` points to a sync working directory with a `main/` prefix, the scaffold auto-detects it and nests files correctly
