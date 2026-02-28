# Keboola Agent CLI (`kbagent`) — Plán projektu

## Proč to děláme

AI coding agenti (Claude Code, Codex, Gemini) potřebují jednoduchý způsob, jak interagovat s Keboola projekty. Dnes existují dva nástroje:

1. **Keboola CLI (`kbc`)** — Go, zaměřené na Infrastructure-as-Code workflow (pull/push konfigurací jako soubory). Nepodporuje nativně multi-projekt. Je to nástroj pro lidské vývojáře, ne pro AI agenty.

2. **Keboola MCP Server** — Python, 30+ nástrojů pro AI (konfigurace, flows, joby, SQL dotazy). Ale je single-project a vyžaduje MCP protokol.

### Co chybí

Jednoduchý CLI nástroj, který:
- **Funguje s více Keboola projekty najednou** (různé stacky, různé tokeny)
- **Je optimalizovaný pro AI agenty** (strukturovaný výstup, jasné příkazy, self-documenting)
- **Abstrahuje Keboola API** do jednoduchých příkazů bez nutnosti znát endpointy
- **Poskytuje kontext** — agent se může zeptat "jak tě používám?" a dostane odpověď

### Co chceme vyrobit (MVP)

CLI nástroj `kbagent`, který umí:
1. **Spravovat připojení na projekty** — přidat/odebrat/editovat projekty (API token + stack URL)
2. **Vylistovat připojené projekty** — přehled všech napojených Keboola projektů
3. **Listovat konfigurace** — z jednoho, více, nebo všech projektů najednou
4. **Poskytnout kontext agentům** — příkaz `kbagent context` vypíše instrukce

### Testovací projekty

| Alias | Stack URL | Token prefix |
|-------|-----------|-------------|
| prod-aws | https://connection.keboola.com/ | 901-... |
| dev-azure | https://connection.north-europe.azure.keboola.com/ | 532-... |
| dev-gcp | https://connection.europe-west3.gcp.keboola.com/ | 1796-... |

---

## Tech stack

- **Python 3.12+** s `uv` jako package manager
- **Typer** — CLI framework (Click pod kapotou, type hints = argumenty, auto-complete, auto-help, ~40-50% méně boilerplatu než Click)
- **Rich** — formátování výstupu (tabulky, barvy, panely) — od stejného autora jako Typer (Textualize)
- **httpx** — HTTP klient (sync pro MVP, async-ready pro multi-projekt fan-out)
- **Pydantic 2.x** — modely a validace konfigurace
- **platformdirs** — cross-platform config cesty

Instalace: `uv tool install .` → příkaz `kbagent` globálně dostupný.

### Reasoning: Proč Typer místo Click

Na základě analýzy CLI frameworků (z projektu Osiris):
- **argparse** — stdlib, ale verbose, žádné auto-complete, ruční subcommands
- **Click** — osvědčený, ale verbose dekorátory
- **Typer** — Click pod kapotou, ale type hints místo dekorátorů → méně kódu, lepší IDE podpora, auto-generated help, shell auto-completion zdarma
- **Textual** — overkill pro CLI, vhodné pro TUI dashboardy

Typer je jasný vítěz pro tento typ projektu.

### Reasoning: Proč Python

- Keboola MCP server je v Pythonu → sdílené patterns, httpx klient
- Rychlý vývoj MVP
- `uv` řeší distribuci (`uv tool install`) srovnatelně s Go binary
- Bohatý ekosystém (Pydantic, Rich, httpx)

---

## Příkazy (MVP)

```
kbagent project list                    # Vylistovat připojené projekty
kbagent project add --alias X --url Y --token Z   # Přidat projekt
kbagent project remove --alias X        # Odebrat projekt
kbagent project edit --alias X          # Editovat projekt
kbagent project status                  # Otestovat konektivitu

kbagent config list [--project X] [--component-type Y]   # Listovat konfigurace
kbagent config detail --project X --component-id Y --config-id Z  # Detail

kbagent context                         # Instrukce pro AI agenty (jak používat CLI)
kbagent doctor                          # Kontrola konektivity, config, permissions najednou
```

Poznámka: `project edit` je plně flag-based (ne interaktivní) — agenti potřebují non-interactive příkazy.

Globální flagy: `--json` / `-j` (strojově čitelný výstup), `--verbose` / `-v`, `--no-color` (vypne Rich formátování, auto-detect non-TTY).

### Exit codes

| Kód | Význam |
|-----|--------|
| 0 | Úspěch |
| 1 | Obecná chyba |
| 2 | Chyba použití (špatné argumenty) |
| 3 | Chyba autentizace (neplatný/expirovaný token) |
| 4 | Síťová chyba (timeout, nedostupný server) |
| 5 | Chyba konfigurace (poškozený config.json) |

### JSON output kontrakt

Všechny příkazy s `--json` vrací konzistentní strukturu:

**Úspěch:**
```json
{
  "status": "ok",
  "data": [ ... ]
}
```

**Chyba:**
```json
{
  "status": "error",
  "error": {
    "code": "INVALID_TOKEN",
    "message": "Token is invalid or expired",
    "project": "prod-aws",
    "retryable": false
  }
}
```

Token se v chybových zprávách nikdy netiskne celý, vždy maskovaný: `901-...pt0k`.

### Typer ukázka (styl kódu)

```python
import typer
from rich.console import Console

app = typer.Typer(name="kbagent", help="Keboola Agent CLI — AI-friendly interface to Keboola projects")
project_app = typer.Typer(help="Manage connected Keboola projects")
config_app = typer.Typer(help="Browse and inspect configurations")
app.add_typer(project_app, name="project")
app.add_typer(config_app, name="config")

@project_app.command("add")
def project_add(
    alias: str = typer.Option(..., help="Human-friendly name for this project"),
    url: str = typer.Option("https://connection.keboola.com", help="Keboola stack URL"),
    token: str = typer.Option(..., help="Storage API token"),
    json_output: bool = typer.Option(False, "--json", "-j", help="JSON output"),
):
    """Add a new Keboola project connection."""
    result = project_service.add_project(alias, url, token)
    output(result, json_output)
```

---

## Architektura (3-vrstvá)

### Reasoning: Proč 3 vrstvy

Požadavek: pokud se změní Keboola API, opravuje se na **jednom místě**, ne po celé codebase. Proto oddělujeme:

```
CLI commands  →  Services (business logika)  →  API Client (HTTP)
(Typer, output)    (agregace, resolving)         (endpointy, requesty)
```

- **API se změní** → upravuje se pouze `client.py`
- **Business logika se změní** → upravuje se pouze `services/`
- **UI se změní** → upravuje se pouze `commands/`

### Struktura projektu

```
keboola_agent_cli/
├── pyproject.toml
├── README.md
├── CLAUDE.md                          # Kontext pro Claude Code agenta
├── docs/
│   └── PLAN.md                        # Tento dokument
├── .gitignore
├── .python-version                    # 3.12
├── src/keboola_agent_cli/
│   ├── __init__.py                    # __version__
│   ├── __main__.py                    # python -m support
│   ├── cli.py                         # Typer app, subcommand groups, globální callback
│   ├── commands/                      # VRSTVA 1: CLI (tenká, jen Typer + output)
│   │   ├── __init__.py
│   │   ├── project.py                 #   parsuje argumenty, volá service, formátuje output
│   │   ├── config.py                  #   parsuje argumenty, volá service, formátuje output
│   │   └── context.py                 #   statický text s instrukcemi
│   ├── services/                      # VRSTVA 2: Business logika (jádro aplikace)
│   │   ├── __init__.py
│   │   ├── project_service.py         #   add/remove/edit/status projekty, validace
│   │   └── config_service.py          #   list/detail configs, multi-projekt agregace, filtrování
│   ├── client.py                      # VRSTVA 3: HTTP klient (jediné místo s API endpointy)
│   ├── models.py                      # Pydantic modely (sdílené napříč vrstvami)
│   ├── config_store.py                # Persistence config.json (používá services vrstva)
│   ├── output.py                      # OutputFormatter (JSON vs Rich tabulky)
│   └── errors.py                      # KeboolaApiError, ConfigError
└── tests/
    ├── conftest.py
    ├── test_cli.py                    # End-to-end CLI testy (CliRunner)
    ├── test_services.py               # Business logika testy
    ├── test_client.py                 # API klient testy (mockovaný HTTP)
    └── test_config_store.py           # Config persistence testy
```

### Odpovědnosti vrstev

**`commands/` (CLI vrstva)** — Tenká. Jen Typer dekorátory, parsování argumentů, volání service, formátování výstupu přes `OutputFormatter`. Žádná business logika.

**`services/` (Business logika)** — Jádro aplikace. Řeší:
- `project_service.py`: Validace projektů (volá `client.verify_token()`), přidávání/odebírání z config store
- `config_service.py`: Resolving projektů (jeden/více/všechny), agregace konfigurací, filtrování podle typu/komponenty

**`client.py` (API vrstva)** — Jediné místo, kde se mluví s Keboola API. Všechny endpointy, HTTP hlavičky, URL konstrukce. Pokud se API změní, mění se **jen tento soubor**. Obsahuje:
- Centrální timeout konfigurace (connect: 5s, read: 30s)
- Retry strategie s exponential backoff pro 429/5xx (max 3 pokusy)
- Response → Pydantic model mapping (DTO boundary), aby API shape changes neunikly do services
- Token masking v chybových zprávách (nikdy plný token v logu)

**`models.py`** — Sdílené Pydantic modely. Definují datový kontrakt mezi vrstvami.

---

## Konfigurace

Uložení: `~/.config/keboola-agent-cli/config.json` (oprávnění `0600`)

```json
{
  "version": 1,
  "default_project": "prod-aws",
  "projects": {
    "prod-aws": {
      "stack_url": "https://connection.keboola.com",
      "token": "901-...",
      "project_name": "My Project",
      "project_id": 1234
    }
  }
}
```

### Token storage strategie

1. **MVP**: Tokeny v plain text config.json s `0600` permissions (industry standard — stejně jako `~/.aws/credentials`)
2. **Env variable override**: `KBC_TOKEN` a `KBC_STORAGE_API_URL` pro CI/CD a dočasná prostředí (token se nepíše na disk)
3. **Post-MVP**: Volitelná integrace `keyring` pro OS keychain (macOS Keychain, Linux Secret Service)

Token se **nikdy** netiskne celý — v logech, chybách a výstupu vždy maskovaný (`901-...pt0k`).

### Config migrace

Pole `"version": 1` slouží pro budoucí migraci schématu. `ConfigStore` při načtení zkontroluje verzi a provede upgrade pokud je potřeba.

### Reasoning: Proč JSON a ne TOML/YAML

- JSON je nativní pro Pydantic (`model_dump_json()` / `model_validate_json()`)
- Agenti snadno parsují JSON
- Žádná extra závislost
- Config je malý, nepotřebuje komentáře

---

## Keboola API endpointy

| Operace | Endpoint | Popis |
|---------|----------|-------|
| Ověření tokenu | `GET {stack}/v2/storage/tokens/verify` | Validace + info o projektu |
| Seznam komponent | `GET {stack}/v2/storage/components?include=configuration` | Komponenty s konfiguracemi |
| Detail konfigurace | `GET {stack}/v2/storage/components/{id}/configs/{configId}` | Plný detail |

Auth: header `X-StorageApi-Token: {token}`

---

## Fáze implementace

<!-- PHASE:1 -->
## Phase 1: Project Skeleton

### Branch
`phase-1-skeleton`

### Scope
Initialize the Python project with `uv`, create the complete directory structure, Typer CLI skeleton with all command groups, shared models, output formatter, and error handling. After this phase, `kbagent --help` works and shows all command groups.

Key details:
- Use `uv init` and configure `pyproject.toml` with entry point `[project.scripts] kbagent = "keboola_agent_cli.cli:app"`
- Dependencies: `typer[all]>=0.12`, `rich>=13`, `httpx>=0.27`, `pydantic>=2.5`, `platformdirs>=4`
- Dev dependencies: `pytest>=8`, `pytest-httpx>=0.30`
- Python version: 3.12
- `OutputFormatter` class in `output.py`: takes `json_mode: bool`, method `output(data, human_formatter)` — if json_mode, prints JSON to stdout; else calls human_formatter with Rich Console. Method `error(message)` for errors. Method `success(message)` for success messages.
- `errors.py`: `KeboolaApiError(message, status_code, error_code, retryable)` and `ConfigError(message)`. Helper `mask_token(token)` → `"{prefix}-...{last4}"`.
- `models.py`: Pydantic models `ProjectConfig(stack_url, token, project_name, project_id)`, `AppConfig(version=1, default_project, projects: dict[str, ProjectConfig])`. Also `ErrorResponse(code, message, project, retryable)` and `SuccessResponse(status="ok", data)`.
- `cli.py`: Typer app with global callback for `--json`, `--verbose`, `--no-color`. Subgroups: `project`, `config`. Top-level commands: `context`, `doctor`. Auto-detect non-TTY to disable Rich.
- All commands in `commands/` are stubs that print "Not yet implemented" — but they must exist with correct Typer signatures and help text.

### Files to Create/Modify
- `pyproject.toml` — project metadata, deps, entry point
- `.python-version` — contains `3.12`
- `.gitignore` — Python standard + `.env`, `dist/`, `*.egg-info`
- `src/keboola_agent_cli/__init__.py` — `__version__ = "0.1.0"`
- `src/keboola_agent_cli/__main__.py` — `from .cli import app; app()`
- `src/keboola_agent_cli/cli.py` — Typer root app, global options callback, register all subgroups
- `src/keboola_agent_cli/commands/__init__.py` — empty
- `src/keboola_agent_cli/commands/project.py` — stub commands: add, list, remove, edit, status
- `src/keboola_agent_cli/commands/config.py` — stub commands: list, detail
- `src/keboola_agent_cli/commands/context.py` — stub command
- `src/keboola_agent_cli/commands/doctor.py` — stub command
- `src/keboola_agent_cli/services/__init__.py` — empty
- `src/keboola_agent_cli/models.py` — Pydantic models: AppConfig, ProjectConfig, ErrorResponse, SuccessResponse
- `src/keboola_agent_cli/config_store.py` — empty class skeleton with load/save signatures
- `src/keboola_agent_cli/output.py` — OutputFormatter with json/rich dual mode
- `src/keboola_agent_cli/errors.py` — KeboolaApiError, ConfigError, mask_token()
- `tests/__init__.py` — empty
- `tests/conftest.py` — basic fixtures (tmp config dir)

### Acceptance Criteria
- [ ] `uv pip install -e .` succeeds without errors
- [ ] `kbagent --help` shows app description, lists `project`, `config`, `context`, `doctor` commands
- [ ] `kbagent project --help` shows subcommands: add, list, remove, edit, status
- [ ] `kbagent config --help` shows subcommands: list, detail
- [ ] `kbagent --json project list` outputs valid JSON (even if just `{"status": "ok", "data": []}`)
- [ ] `kbagent --no-color --help` works (no Rich markup in output)
- [ ] `mask_token("901-10493007-VDtlEDWDF6Tx5V8jjE8FshFlqM0Hl0c08KHqpt0k")` returns `"901-...pt0k"`
- [ ] All Pydantic models can be instantiated and serialized to JSON
- [ ] Project structure matches the plan (all directories and files exist)

### Tests Required
- `tests/test_errors.py`: test `mask_token()` with various inputs (short token, normal token, empty string)
- `tests/test_models.py`: test `AppConfig` and `ProjectConfig` serialization/deserialization round-trip
- `tests/test_output.py`: test `OutputFormatter` JSON mode outputs valid JSON, human mode doesn't crash
<!-- /PHASE:1 -->

<!-- PHASE:2 -->
## Phase 2: Project Management

### Branch
`phase-2-projects`

### Scope
Implement the full project management lifecycle: add, list, remove, edit, status. This includes the Keboola API client (`client.py`), config persistence (`config_store.py`), project service (`project_service.py`), and the CLI commands in `commands/project.py`.

Key details:
- `client.py` — `KeboolaClient(stack_url, token)`:
  - Uses `httpx.Client` with timeout `httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)`
  - Retry: wrap requests with max 3 attempts, exponential backoff (1s, 2s, 4s) for status 429, 500, 502, 503, 504
  - `User-Agent: keboola-agent-cli/{version}`
  - `verify_token()` → `GET /v2/storage/tokens/verify` → returns Pydantic model with project name, id, token description
  - `list_components(component_type=None)` → `GET /v2/storage/components?include=configuration` (optionally `&componentType={type}`)
  - `get_config_detail(component_id, config_id)` → `GET /v2/storage/components/{component_id}/configs/{config_id}`
  - All errors wrapped in `KeboolaApiError` with masked token
- `config_store.py` — `ConfigStore(config_dir=None)`:
  - Default dir: `platformdirs.user_config_dir("keboola-agent-cli")`
  - `load()` → reads `config.json`, returns `AppConfig` (or empty AppConfig if file doesn't exist)
  - `save(config)` → writes JSON with indent=2, sets file permissions 0o600
  - `add_project(alias, project)`, `remove_project(alias)`, `edit_project(alias, **kwargs)`, `get_project(alias)`
  - Checks config version on load
- `services/project_service.py` — `ProjectService(config_store, client_factory)`:
  - `client_factory` is a callable `(stack_url, token) -> KeboolaClient` for DI
  - `add_project(alias, stack_url, token)` → calls `client.verify_token()`, extracts project name/id, saves to config_store
  - `remove_project(alias)` → removes from config_store
  - `edit_project(alias, stack_url=None, token=None)` → updates fields, re-verifies if token changed
  - `list_projects()` → returns list of projects from config
  - `get_status(aliases=None)` → for each project, calls `verify_token()`, returns status (ok/error + response time)
- `commands/project.py` — all Typer commands, fully flag-based (non-interactive):
  - `add --alias X --url Y --token Z` (alias and token required, url has default)
  - `list` (no args)
  - `remove --alias X` (alias required)
  - `edit --alias X [--url Y] [--token Z]` (alias required, others optional)
  - `status [--project X]` (optional, defaults to all)
  - All commands respect `--json` global flag from context
  - Proper exit codes: 0 success, 3 auth error, 4 network error, 5 config error
- Support `KBC_TOKEN` and `KBC_STORAGE_API_URL` env vars as override/fallback

### Files to Create/Modify
- `src/keboola_agent_cli/client.py` — full KeboolaClient implementation
- `src/keboola_agent_cli/config_store.py` — full ConfigStore implementation
- `src/keboola_agent_cli/services/project_service.py` — full ProjectService
- `src/keboola_agent_cli/commands/project.py` — replace stubs with real implementation
- `src/keboola_agent_cli/cli.py` — wire up context object (config_store, output_formatter) in global callback
- `src/keboola_agent_cli/models.py` — add `TokenVerifyResponse` model if needed

### Acceptance Criteria
- [ ] `kbagent project add --alias test --url https://connection.keboola.com --token 901-10493007-VDtlEDWDF6Tx5V8jjE8FshFlqM0Hl0c08KHqpt0k` succeeds, verifies token via API, saves to config.json
- [ ] `kbagent project list` shows the added project in a Rich table
- [ ] `kbagent --json project list` outputs valid JSON with project details
- [ ] `kbagent project status` shows connectivity status (OK/ERROR) with response time
- [ ] `kbagent project remove --alias test` removes the project
- [ ] `kbagent project edit --alias test --url https://new.url.com` updates the URL
- [ ] Config file at `~/.config/keboola-agent-cli/config.json` has permissions 0600
- [ ] Token is never printed fully in any output — always masked
- [ ] Invalid token returns exit code 3 with structured error in `--json` mode
- [ ] Network timeout returns exit code 4
- [ ] Retry works: client retries on 5xx (verifiable via test mock)

### Tests Required
- `tests/test_client.py`: test `verify_token()` success, test 401 error, test retry on 503 (mock httpx), test timeout handling, test token masking in errors
- `tests/test_config_store.py`: test load empty config, test add/remove/edit project, test file permissions, test version check
- `tests/test_services.py`: test `ProjectService.add_project()` with mock client (success + failure), test `list_projects()`, test `get_status()` with mixed success/failure
- `tests/test_cli.py`: test `project add` via CliRunner (mock the service), test `project list` output in json and human mode
<!-- /PHASE:2 -->

<!-- PHASE:3 -->
## Phase 3: Configuration Listing

### Branch
`phase-3-configs`

### Scope
Implement configuration listing across one or multiple projects. This includes config service (`config_service.py`) and the CLI commands in `commands/config.py`.

Key details:
- `services/config_service.py` — `ConfigService(config_store, client_factory)`:
  - `resolve_projects(aliases=None)` → if aliases provided, return those projects; if empty, return all. Raises `ConfigError` if alias not found.
  - `list_configs(aliases=None, component_type=None, component_id=None)` → for each resolved project: create client, call `list_components(component_type)`, flatten into list of dicts with keys: `project_alias`, `component_id`, `component_name`, `component_type`, `config_id`, `config_name`, `config_description`. If `component_id` filter set, only include matching. Per-project errors are reported but don't stop other projects.
  - `get_config_detail(alias, component_id, config_id)` → calls client for single project, returns full config detail
- `commands/config.py`:
  - `list [--project X] [--component-type Y] [--component-id Z]` — `--project` can be repeated (Typer `List[str]`), `--component-type` is `Choice["extractor","writer","transformation","application"]`
  - `detail --project X --component-id Y --config-id Z` — all required
  - Human output: Rich table grouped by project alias with columns: Component, Type, Config ID, Config Name
  - JSON output: `{"status": "ok", "data": [...]}`
  - If a project fails, show error for that project but continue with others

### Files to Create/Modify
- `src/keboola_agent_cli/services/config_service.py` — full ConfigService
- `src/keboola_agent_cli/commands/config.py` — replace stubs with real implementation
- `src/keboola_agent_cli/client.py` — verify `list_components()` and `get_config_detail()` work correctly (already created in Phase 2, may need adjustments)
- `src/keboola_agent_cli/output.py` — add `output_configs()` human formatter (Rich table grouped by project)

### Acceptance Criteria
- [ ] `kbagent config list --json` returns configurations from all connected projects
- [ ] `kbagent config list --project prod-aws --json` returns configs only from that project
- [ ] `kbagent config list --project prod-aws --project dev-azure` returns configs from both projects
- [ ] `kbagent config list --component-type extractor` filters by component type
- [ ] `kbagent config list --component-id keboola.ex-db-snowflake` filters by specific component
- [ ] `kbagent config list` (human mode) shows Rich table grouped by project
- [ ] `kbagent config detail --project prod-aws --component-id X --config-id Y --json` returns full config detail
- [ ] If one project fails (e.g., expired token), other projects still return results and error is shown for the failed one
- [ ] Unknown project alias returns exit code 5 with helpful error message

### Tests Required
- `tests/test_services.py`: add tests for `ConfigService.list_configs()` — multi-project aggregation, filtering by type, filtering by component_id, partial failure (one project errors), empty results
- `tests/test_services.py`: add test for `ConfigService.get_config_detail()`
- `tests/test_cli.py`: add tests for `config list` via CliRunner — json output, human output, project filter, type filter, error handling
<!-- /PHASE:3 -->

<!-- PHASE:4 -->
## Phase 4: Agent Context, Doctor, and Polish

### Branch
`phase-4-context-doctor`

### Scope
Implement `kbagent context` (agent instructions), `kbagent doctor` (health check), CLAUDE.md, and README.md. Polish overall UX: consistent error messages, exit codes, Rich formatting.

Key details:
- `commands/context.py` — `kbagent context` outputs a curated text block with:
  - What kbagent is
  - All commands with examples (copy-pasteable)
  - Tips: always use `--json`, how to parse output, common workflows
  - Exit codes table
  - This text should be useful for ANY AI agent (Claude, Codex, Gemini)
- `commands/doctor.py` — `kbagent doctor`:
  - Check 1: Config file exists and has correct permissions (0600)
  - Check 2: Config file is valid JSON and parseable
  - Check 3: For each project, verify token (API call) — show OK/FAIL with response time
  - Check 4: CLI version
  - Output: Rich panel with check results (green/red), or JSON with `--json`
- `CLAUDE.md` at project root:
  - How to build: `uv pip install -e ".[dev]"`
  - How to run: `kbagent --help`
  - How to test: `pytest tests/ -v`
  - Project structure overview
  - Coding conventions (Typer commands, 3-layer arch, Pydantic models)
- `README.md`:
  - Project description, installation, quick start with examples
  - All commands documented
  - Architecture overview (brief)

### Files to Create/Modify
- `src/keboola_agent_cli/commands/context.py` — full implementation with comprehensive agent instructions
- `src/keboola_agent_cli/commands/doctor.py` — full health check implementation
- `src/keboola_agent_cli/cli.py` — ensure doctor and context are registered (should already be from Phase 1)
- `CLAUDE.md` — new file, project development context
- `README.md` — new file, user-facing documentation

### Acceptance Criteria
- [ ] `kbagent context` outputs comprehensive usage instructions including all commands with examples
- [ ] `kbagent context` text mentions `--json` flag, exit codes, and common workflows
- [ ] `kbagent doctor` checks config file existence, permissions, validity
- [ ] `kbagent doctor` tests connectivity to all configured projects
- [ ] `kbagent doctor --json` outputs structured JSON with all check results
- [ ] `CLAUDE.md` exists with build/run/test instructions and coding conventions
- [ ] `README.md` exists with installation, quick start, and all commands documented
- [ ] All exit codes are consistent across all commands (0=ok, 1=general, 2=usage, 3=auth, 4=network, 5=config)
- [ ] `--no-color` flag works correctly on all commands
- [ ] Non-TTY detection auto-disables Rich formatting

### Tests Required
- `tests/test_cli.py`: add test for `context` command output (contains key phrases like "kbagent", "--json")
- `tests/test_cli.py`: add test for `doctor` command — mock config and API, verify checks run
- `tests/test_cli.py`: add test for `--no-color` flag
- `tests/test_cli.py`: add test for exit codes on auth error (3), network error (4), config error (5)
<!-- /PHASE:4 -->

<!-- PHASE:5 -->
## Phase 5: Integration Tests and Final Verification

### Branch
`phase-5-tests-final`

### Scope
Comprehensive test suite covering all layers, plus final verification that everything works end-to-end. Ensure all existing tests still pass, add missing coverage, and run full integration test with real API (using test projects).

Key details:
- Review all existing tests from Phases 1-4, fix any that are broken
- Add integration test file that can optionally run against real Keboola API (skip if no env vars)
- Ensure test coverage for edge cases: empty project list, invalid JSON in config file, corrupted config, concurrent access
- Run `ruff check` and `ruff format` for code quality
- Final manual verification with all 3 test projects

### Files to Create/Modify
- `tests/test_integration.py` — optional integration tests (skipped without env vars `KBA_TEST_TOKEN_*`)
- `tests/test_client.py` — add edge case tests: malformed JSON response, empty response, large response
- `tests/test_config_store.py` — add edge cases: corrupted file, missing directory, permission denied
- `tests/test_cli.py` — ensure all commands have at least basic test coverage
- `pyproject.toml` — add `[tool.ruff]` and `[tool.pytest.ini_options]` configuration if missing

### Acceptance Criteria
- [ ] `pytest tests/ -v` — all tests pass (0 failures)
- [ ] `ruff check src/ tests/` — no linting errors
- [ ] `ruff format --check src/ tests/` — all files formatted
- [ ] `kbagent project add` works with all 3 test projects (real API)
- [ ] `kbagent project list` shows all 3 projects
- [ ] `kbagent project status` shows OK for all 3
- [ ] `kbagent config list --json` returns configurations from all 3 projects
- [ ] `kbagent config list --project prod-aws` returns configs from one project
- [ ] `kbagent config list --component-type extractor` filters correctly
- [ ] `kbagent doctor` shows all green
- [ ] `kbagent context` outputs complete agent instructions
- [ ] No token is printed in full anywhere in any output

### Tests Required
- `tests/test_integration.py`: integration test `test_full_workflow()` — add project, list, status, config list, remove (requires env vars, skipped otherwise)
- All existing tests from Phases 1-4 pass
- Edge case tests for client, config_store, and CLI as described above
<!-- /PHASE:5 -->

---

## Klíčové design patterns

### 1. 3-vrstvá architektura
`commands/` → `services/` → `client.py`. API změny = jen `client.py`. Business logika = jen `services/`.

### 2. Dual output
`--json` → čistý JSON na stdout (pro agenty); bez flagu → Rich tabulky (pro lidi).

### 3. Project resolution (v `services/`)
`--project X` filtruje na konkrétní alias. Bez `--project` = všechny projekty.

### 4. Error handling
Dekorátor v CLI vrstvě chytající `KeboolaApiError`/`ConfigError`. V `--json` mode vrací strukturovaný error s `code`, `message`, `retryable`. Vždy vrací správný exit code (viz tabulka výše).

### 5. Config store
Pydantic model ↔ JSON soubor, permissions `0600`, atomický save. Podpora verzování schématu pro budoucí migrace.

### 6. Dependency injection
Services přijímají `KeboolaClient` a `ConfigStore` jako parametry → snadné testování s mocky.

### 7. Retry a timeout
Centrální v `client.py`: connect timeout 5s, read timeout 30s, exponential backoff pro 429/5xx (max 3 pokusy).

### 8. Token masking
Tokeny se v logech, chybách a výstupech nikdy nezobrazují celé. Vždy `{prefix}-...{last4}`.

---

## Ověření (po implementaci)

1. `uv pip install -e .` && `kbagent --help` — CLI se nainstaluje a zobrazí help
2. Přidat 3 testovací projekty přes `kbagent project add`
3. `kbagent project list` — zobrazí 3 projekty v tabulce
4. `kbagent project list --json` — JSON výstup
5. `kbagent project status` — ověří konektivitu ke všem 3
6. `kbagent config list --json` — konfigurace ze všech projektů
7. `kbagent config list --project prod-aws` — konfigurace jen z jednoho
8. `kbagent config list --component-type extractor` — filtr podle typu
9. `kbagent context` — instrukce pro agenty
10. `pytest tests/` — všechny testy projdou
11. `kbagent doctor` — zelený stav pro všechny kontroly

---

## Peer review (3 AI provideři)

Plán byl zrecenzován třemi nezávislými AI:

| Provider | Model | Hodnocení | Klíčové nápady |
|----------|-------|-----------|----------------|
| **Anthropic** | Claude Sonnet 4.5 | 8.5/10 | Exit codes, error schema, retry logic, `--dry-run` |
| **Google** | Gemini | 9/10 | `doctor` command, `--fields` flag, paginace, env var override |
| **OpenAI** | GPT-5.3 Codex | 7.5/10 | JSON contract spec, DTO boundary, token masking, golden tests, config migrace |

### Konsensus (zapracováno do plánu)
- **Strukturované JSON chyby** s `code`, `message`, `retryable` (3/3)
- **Exit codes** definované a zdokumentované (2/3)
- **Retry/timeout strategie** centrální v client.py (2/3)
- **Token masking** v logech a chybách (2/3)
- **`kbagent doctor`** command (2/3)
- **`--no-color` / auto-detect TTY** (1/3, ale best practice)
- **Env variable override** pro CI/CD (1/3, ale praktické)
- **Config schema migrace** (1/3, ale forward-thinking)

### Vědomě odloženo na post-MVP
- Async/concurrent multi-project fetching (všichni zmínili, ale sync stačí pro 3-10 projektů)
- `keyring` integrace pro OS keychain
- `--fields` flag pro selektivní výstup
- `kbagent config schema` command
- Formální JSON schema spec (`docs/JSON_SCHEMA.md`) s golden tests
- Paginace (Keboola Storage API typicky vrací stovky, ne tisíce položek)
