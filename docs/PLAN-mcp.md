# Phase 7: MCP Integration — Multi-projektový wrapper nad Keboola MCP

## Proč to děláme

kbagent je CLI pro AI agenty s klíčovou vlastností: **agent neví o projektech**. Zavolá `kbagent job list` a dostane joby ze VŠECH připojených projektů najednou. Žádná iterace, žádné tokeny, žádné stacky.

Problém: kbagent aktuálně pokrývá zlomek Keboola API (project mgmt, config listing, job listing). Keboola MCP server (`keboola-mcp-server`, 31 toolů) umí mnohem víc — SQL dotazy, spouštění jobů, vytváření konfigurací, flows, data apps, search, storage management.

Řešení: kbagent použije Keboola MCP server jako **backend** a vystaví jeho tooly přes CLI. Agent zavolá `kbagent tool call get_tables` a dostane tabulky ze všech projektů. Feature parity s MCP, ale multi-projektově a přes CLI.

---

## Jak to funguje pro agenta

```bash
# DISCOVER — agent zjistí co může dělat
kbagent --json tool list
# → vypíše všech 31 MCP toolů s popisem a input schematem

# READ TOOLY — automaticky přes VŠECHNY projekty
kbagent --json tool call get_tables
# → vrátí tabulky ze všech 4 projektů, každý výsledek anotovaný project_alias

kbagent --json tool call get_jobs --input '{"status": "error", "limit": 10}'
# → vrátí error joby ze všech projektů

kbagent --json tool call search --input '{"patterns": ["alza"], "limit": 10}'
# → prohledá všechny projekty

# FILTR NA KONKRÉTNÍ PROJEKT (volitelný)
kbagent --json tool call get_tables --project hlicas
# → pouze tabulky z projektu hlicas

# WRITE TOOLY — použijí default projekt (nebo --project)
kbagent --json tool call run_job \
  --input '{"component_id": "keboola.ex-db-snowflake", "configuration_id": "123"}'
# → spustí job v default projektu

kbagent --json tool call create_sql_transformation --project hlicas \
  --input '{"name": "My Transform", "description": "...", "sql_code_blocks": [...]}'
# → vytvoří transformaci v projektu hlicas

# SQL DOTAZ (vždy single-project)
kbagent --json tool call query_data --project hlicas \
  --input '{"sql_query": "SELECT * FROM my_table LIMIT 10", "query_name": "test"}'
```

---

## Architektura

```
AI Agent
  │  (volá kbagent CLI příkazy v terminálu)
  ▼
kbagent CLI
  ├── tool list          ← zjistí dostupné MCP tooly
  ├── tool call <name>   ← zavolá MCP tool
  │
  ▼
commands/tool.py  (CLI vrstva — parsuje argumenty, formátuje output)
  │
  ▼
services/mcp_service.py  (business logika — multi-projekt routing + agregace)
  │
  ├── Read tool?  → spustí N MCP subprocess PARALELNĚ (asyncio.gather)
  │     ├── keboola-mcp-server  [token=901-xxx, url=keboola.com]       ── prod-aws
  │     ├── keboola-mcp-server  [token=532-xxx, url=azure.keboola.com] ── dev-azure
  │     ├── keboola-mcp-server  [token=1796-xxx, url=gcp.keboola.com]  ── dev-gcp
  │     └── keboola-mcp-server  [token=641-xxx, url=gcp.keboola.com]   ── hlicas
  │     Výsledky agregované s project_alias, chyby per-projekt sbírané do errors[]
  │
  └── Write tool? → spustí 1 MCP subprocess (default/specifikovaný projekt)
        └── keboola-mcp-server  [token pro cílový projekt]
```

### Komunikace s MCP serverem

Používáme **MCP Python SDK** (`mcp` package na PyPI):

```python
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# 1. Start MCP server jako subprocess (stdio transport)
server_params = StdioServerParameters(
    command="uvx",
    args=["keboola_mcp_server"],
    env={
        "KBC_STORAGE_TOKEN": project.token,
        "KBC_STORAGE_API_URL": project.stack_url,
    },
)

# 2. Připojení přes async context manager
async with AsyncExitStack() as stack:
    transport = await stack.enter_async_context(stdio_client(server_params))
    read_stream, write_stream = transport
    session = await stack.enter_async_context(ClientSession(read_stream, write_stream))
    await session.initialize()

    # 3. List tools
    tools = await session.list_tools()

    # 4. Call tool
    result = await session.call_tool("get_tables", {"bucket_ids": []})
```

SDK je **async-only**. Typer je sync. Řešení: sync wrappery v service vrstvě přes `asyncio.run()`.

---

## Read vs Write klasifikace

MCP tooly se dělí na dvě kategorie podle toho, zda dává smysl je agregovat přes projekty:

### Read tooly (multi-project — všechny projekty paralelně)

| Tool | Popis |
|------|-------|
| `get_components` | Seznam komponent s metadaty |
| `get_configs` | Konfigurace komponent |
| `get_config_examples` | Ukázkové konfigurace |
| `get_buckets` | Storage buckety |
| `get_tables` | Tabulky s column info |
| `get_jobs` | Historie jobů s filtrováním |
| `get_flows` | Flow konfigurace |
| `get_flow_examples` | Ukázkové flow konfigurace |
| `get_flow_schema` | JSON schema pro flow typy |
| `get_data_apps` | Data app konfigurace |
| `get_project_info` | Info o projektu |
| `search` | Fulltext hledání across entities |
| `find_component_id` | Najdi komponentu podle jména |
| `docs_query` | Dotaz do Keboola dokumentace |

### Write tooly (single-project — default nebo `--project`)

| Tool | Popis |
|------|-------|
| `create_config` | Vytvoř konfiguraci |
| `add_config_row` | Přidej row do konfigurace |
| `update_config` | Uprav konfiguraci |
| `update_config_row` | Uprav config row |
| `create_sql_transformation` | Vytvoř SQL transformaci |
| `update_sql_transformation` | Uprav SQL transformaci |
| `update_descriptions` | Uprav popisy storage entit |
| `create_flow` | Vytvoř flow |
| `create_conditional_flow` | Vytvoř podmíněný flow |
| `modify_flow` | Uprav flow |
| `update_flow` | Aktualizuj flow konfiguraci |
| `run_job` | Spusť job |
| `modify_data_app` | Vytvoř/uprav data app |
| `deploy_data_app` | Nasaď/zastav data app |
| `create_oauth_url` | Vygeneruj OAuth URL |
| `query_data` | SQL dotaz (běží v jednom DWH) |

### Klasifikační logika

```python
WRITE_PREFIXES = ("create_", "update_", "modify_", "run_", "deploy_", "add_")
SINGLE_PROJECT_TOOLS = ("query_data", "create_oauth_url")

def is_read_tool(tool_name: str) -> bool:
    if tool_name in SINGLE_PROJECT_TOOLS:
        return False
    return not any(tool_name.startswith(p) for p in WRITE_PREFIXES)
```

---

## Formát výstupu

### Read tool (multi-project) — agregovaný výsledek

```json
{
  "status": "ok",
  "data": {
    "results": [
      {
        "project_alias": "prod-aws",
        "project_id": 901,
        "content": [
          {"type": "text", "text": "... MCP tool response ..."}
        ],
        "isError": false
      },
      {
        "project_alias": "hlicas",
        "project_id": 641,
        "content": [
          {"type": "text", "text": "... MCP tool response ..."}
        ],
        "isError": false
      }
    ],
    "errors": [
      {
        "project_alias": "dev-azure",
        "error_code": "CONNECTION_ERROR",
        "message": "Connection timeout after 30s"
      }
    ]
  }
}
```

### Write tool (single-project) — přímý výsledek

```json
{
  "status": "ok",
  "data": {
    "project_alias": "hlicas",
    "project_id": 641,
    "content": [
      {"type": "text", "text": "... MCP tool response ..."}
    ],
    "isError": false
  }
}
```

### Tool list

```json
{
  "status": "ok",
  "data": {
    "tools": [
      {
        "name": "get_tables",
        "description": "Retrieve table details with column info...",
        "inputSchema": {
          "type": "object",
          "properties": {
            "bucket_ids": {"type": "array", "items": {"type": "string"}},
            "table_ids": {"type": "array", "items": {"type": "string"}},
            "include_usage": {"type": "boolean", "default": false}
          }
        },
        "multi_project": true
      },
      {
        "name": "run_job",
        "description": "Start a new job execution...",
        "inputSchema": {
          "type": "object",
          "properties": {
            "component_id": {"type": "string"},
            "configuration_id": {"type": "string"}
          },
          "required": ["component_id", "configuration_id"]
        },
        "multi_project": false
      }
    ]
  }
}
```

Tool list obsahuje `multi_project: true/false` aby agent věděl, které tooly poběží přes všechny projekty a které vyžadují `--project`.

---

## Implementace — soubory

### Nová závislost

**`pyproject.toml`** — přidat do dependencies:
```toml
"mcp>=1.0.0",
```

### Nové soubory

#### `src/keboola_agent_cli/services/mcp_service.py`

MCP client service — jádro celé integrace.

**Třída `McpService`:**
- Konstruktor: `__init__(self, config_store: ConfigStore)` — DI pattern jako JobService/ConfigService
- Read/write klasifikace: `is_read_tool(tool_name) -> bool`
- Resolve projektu: `resolve_project(alias: str | None) -> tuple[str, ProjectConfig]` — vrátí default projekt pokud alias=None
- Resolve projektů: `resolve_projects(aliases: list[str] | None) -> dict[str, ProjectConfig]` — všechny pokud aliases=None

**Async core (privátní metody):**
- `_get_server_params(project: ProjectConfig) -> StdioServerParameters` — build params s env vars
- `_connect_and_list_tools(project: ProjectConfig) -> list[dict]` — start subprocess, initialize session, tools/list, cleanup
- `_connect_and_call_tool(project: ProjectConfig, tool_name: str, arguments: dict) -> dict` — start subprocess, initialize, tools/call, cleanup
- `_call_multi_project(projects: dict[str, ProjectConfig], tool_name: str, arguments: dict) -> dict` — `asyncio.gather()` přes všechny projekty, sbírá results[] a errors[]
- `_call_single_project(alias: str, project: ProjectConfig, tool_name: str, arguments: dict) -> dict` — single project call

**Sync public API (volané z commands/):**
- `list_tools(alias: str | None = None) -> list[dict]` — sync wrapper, volá `asyncio.run(self._connect_and_list_tools(...))`
- `call_tool(tool_name: str, arguments: dict | None, aliases: list[str] | None) -> dict` — hlavní metoda:
  1. Rozhodne read vs write
  2. Read: resolve all projects (nebo filtrované), zavolá `_call_multi_project()`
  3. Write: resolve jeden projekt (default nebo --project), zavolá `_call_single_project()`
  4. Vrátí agregovaný výsledek
- `check_mcp_available() -> dict` — zkontroluje zda je `uvx` na PATH (`shutil.which("uvx")`)

**Klíčové detaily:**
- Environment pro subprocess: `KBC_STORAGE_TOKEN` a `KBC_STORAGE_API_URL` z ProjectConfig
- `keboola-mcp-server` se NEINSTALUJE jako dependency kbagent — spouští se přes `uvx keboola_mcp_server` (zero-install)
- Pro paralelní read tooly: `asyncio.gather(*tasks, return_exceptions=True)` — exceptions per projekt se sbírají do errors[] místo aby zabily celý call
- Subprocess lifecycle: start → initialize → call → close. Na každý CLI invocation nový subprocess (žádný daemon).

#### `src/keboola_agent_cli/commands/tool.py`

CLI command vrstva pro MCP tooly.

```python
tool_app = typer.Typer(help="Call Keboola MCP tools across projects")
```

**Příkaz `tool list`:**
```python
@tool_app.command("list")
def tool_list(
    ctx: typer.Context,
    project: str | None = typer.Option(None, "--project", help="Project alias (uses any project to discover tools)"),
) -> None:
    """List available MCP tools with descriptions and parameters."""
```
- Volá `mcp_service.list_tools(alias=project)`
- Human mode: Rich tabulka (Name, Description, Multi-project?)
- JSON mode: strukturovaný výstup s inputSchema

**Příkaz `tool call`:**
```python
@tool_app.command("call")
def tool_call(
    ctx: typer.Context,
    tool_name: str = typer.Argument(..., help="MCP tool name (e.g. get_tables, run_job)"),
    project: list[str] | None = typer.Option(None, "--project", help="Project alias (repeatable for multi-project)"),
    input_json: str | None = typer.Option(None, "--input", help="Tool input as JSON string"),
) -> None:
    """Call an MCP tool. Read tools run across all projects by default."""
```
- Parsuje `--input` JSON (default: `{}`)
- Volá `mcp_service.call_tool(tool_name, arguments, aliases=project)`
- Error handling: ConfigError → exit 5, MCP errors → exit 1
- Human mode: formátovaný výsledek (panel per projekt)
- JSON mode: strukturovaný výstup

#### `tests/test_mcp_service.py`

Unit testy pro McpService s mockovaným MCP subprocess.

**Testovací strategie:**
- Mock `stdio_client` a `ClientSession` z MCP SDK
- Netestujeme reálné MCP subprocess (to je integrační test)
- Testujeme business logiku: read/write klasifikaci, multi-project agregaci, error handling

**Testy:**
- `test_is_read_tool_*` — klasifikace toolů (get_tables=read, run_job=write, query_data=write, search=read)
- `test_list_tools` — mockovaný session.list_tools() vrátí tool list
- `test_call_tool_read_multi_project` — read tool volá všechny projekty, agreguje výsledky
- `test_call_tool_read_filtered_project` — read tool s --project filtruje
- `test_call_tool_write_default_project` — write tool použije default projekt
- `test_call_tool_write_explicit_project` — write tool s --project
- `test_call_tool_write_no_default_error` — write tool bez default projektu → error
- `test_call_tool_partial_failure` — jeden projekt failne, ostatní vrátí výsledky + error v errors[]
- `test_call_tool_all_failures` — všechny projekty failnou → errors[] list
- `test_check_mcp_available` — mock shutil.which
- `test_call_tool_empty_input` — default empty dict pokud --input není zadán

### Modifikované soubory

#### `src/keboola_agent_cli/cli.py`

Přidat 3 řádky:
```python
# Importy (přidat k existujícím)
from .commands.tool import tool_app
from .services.mcp_service import McpService

# Registrace (přidat za job_app)
app.add_typer(tool_app, name="tool")

# V main() callback (přidat za job_service)
mcp_service = McpService(config_store=config_store)
ctx.obj["mcp_service"] = mcp_service
```

#### `src/keboola_agent_cli/output.py`

Přidat dvě formátovací funkce:

**`format_tools_table(console, data)`:**
- Rich tabulka: Tool Name | Description | Multi-project | Parameters
- Zelená/šedá indikace multi-project
- Parameters zobrazeny zkráceně (jen required params)

**`format_tool_result(console, data)`:**
- Pro multi-project výsledky: panel per projekt s alias jako nadpisem
- Pro single-project: jeden panel
- MCP content (text) formátovaný jako code block pokud vypadá jako JSON
- Errors zvýrazněné žlutě (stejný pattern jako u job list errors)

#### `src/keboola_agent_cli/commands/context.py`

Přidat sekci do `AGENT_CONTEXT` stringu:

```
### MCP Tools (Advanced)

  kbagent tool list [--project NAME]
    List all available MCP tools with descriptions and input schemas.
    Use this to discover what operations are available.
    Example:
      kbagent --json tool list

  kbagent tool call TOOL_NAME [--project NAME] [--input JSON]
    Call any Keboola MCP tool. Read tools (get_*, search, find_*)
    automatically run across all connected projects and aggregate results.
    Write tools (create_*, update_*, run_*, modify_*) target the default
    project unless --project is specified.
    Examples:
      kbagent --json tool call get_tables
      kbagent --json tool call get_jobs --input '{"status": "error", "limit": 5}'
      kbagent --json tool call run_job --project hlicas --input '{"component_id": "...", "configuration_id": "..."}'
      kbagent --json tool call query_data --project prod-aws --input '{"sql_query": "SELECT 1", "query_name": "test"}'
```

#### `src/keboola_agent_cli/commands/doctor.py`

Přidat Check 5: MCP server dostupnost:
```python
def _check_mcp_server(mcp_service: McpService) -> dict[str, Any]:
    """Check 5: keboola-mcp-server availability via uvx."""
    result = mcp_service.check_mcp_available()
    if result["available"]:
        return {"check": "mcp_server", "name": "MCP server", "status": "pass",
                "message": "keboola-mcp-server available via uvx"}
    return {"check": "mcp_server", "name": "MCP server", "status": "warn",
            "message": "keboola-mcp-server not found. Install: pip install keboola-mcp-server"}
```

#### `pyproject.toml`

Přidat `mcp` do dependencies.

#### `tests/test_cli.py`

Přidat testy:
- `TestToolList` — `tool list` přes CliRunner s mockovaným McpService
- `TestToolCall` — `tool call get_tables` (read, multi-project mock)
- `TestToolCall` — `tool call run_job --project X` (write, single-project mock)
- `TestToolCall` — `tool call` bez input → prázdný dict
- `TestToolCall` — `tool call` s invalidním JSON → error

---

## Pořadí implementace

| Krok | Soubor | Popis |
|------|--------|-------|
| 1 | `pyproject.toml` | Přidat `mcp>=1.0.0` do dependencies |
| 2 | `services/mcp_service.py` | MCP client service (async core + sync wrappery) |
| 3 | `commands/tool.py` | CLI příkazy `tool list` a `tool call` |
| 4 | `output.py` | `format_tools_table()` a `format_tool_result()` |
| 5 | `cli.py` | Napojení tool_app + McpService (3 řádky) |
| 6 | `tests/test_mcp_service.py` | Unit testy service vrstvy |
| 7 | `tests/test_cli.py` | CLI testy (TestToolList, TestToolCall) |
| 8 | `commands/context.py` | MCP sekce v AGENT_CONTEXT |
| 9 | `commands/doctor.py` | Check 5: MCP server availability |
| 10 | Ruff + pytest | Linting a ověření |

---

## Ověření

### Prerekvizita
```bash
# Ověřit že uvx umí spustit keboola-mcp-server
uvx keboola_mcp_server --help
```

### Automatické testy
```bash
uv run pytest tests/ -v
uv run ruff check src/ tests/
```

### Manuální testy (s reálnými projekty)
```bash
# 1. Discover toolů
kbagent tool list
kbagent --json tool list

# 2. Multi-projekt read tooly
kbagent --json tool call get_tables
kbagent --json tool call get_buckets
kbagent --json tool call get_jobs --input '{"limit": 5}'
kbagent --json tool call search --input '{"patterns": ["alza"], "limit": 5}'

# 3. Filtrovaný read
kbagent --json tool call get_tables --project hlicas

# 4. Write tooly (single-project)
kbagent --json tool call query_data --project hlicas \
  --input '{"sql_query": "SELECT 1 AS test", "query_name": "test"}'

# 5. Doctor
kbagent doctor
# → měl by ukazovat "MCP server: PASS" (nebo WARN pokud chybí uvx)

# 6. Context
kbagent context | grep "tool call"
# → měl by obsahovat MCP sekci
```

---

## Rizika a mitigace

| Riziko | Dopad | Mitigace |
|--------|-------|----------|
| MCP subprocess start ~1-2s overhead per call | Pomalejší než přímé API | Akceptovatelné pro AI agent; budoucí optimalizace: persistent subprocess/daemon |
| `uvx` nebo `keboola-mcp-server` není nainstalovaný | `tool` příkazy nefungují | `doctor` check + jasná chybová hláška + graceful degradace (ostatní příkazy fungují dál) |
| MCP SDK je async, Typer je sync | Potenciální event loop konflikty | `asyncio.run()` wrapper; nevoláme async z async kontextu |
| MCP server vrací text content (ne structured JSON) | Těžší parsování výsledků | Vracíme raw content; agent si parsuje sám (text je typicky JSON-like) |
| Paralelní subprocess pro 4+ projektů | Paměť, file descriptory | Rozumný limit; většina uživatelů má 2-5 projektů |
| MCP protocol/SDK breaking changes | Kompatibilita | Pinovat `mcp>=1.0.0,<2.0.0`; sledovat SDK releases |

---

## Co zůstává beze změny

Existující příkazy (`project`, `config`, `job`, `doctor`, `context`) fungují dál jako dříve. Nové `tool` příkazy jsou **aditivní** — doplňují je. Agent má volbu:
- Specifické příkazy (`job list`, `config list`) — rychlejší, přímé API volání
- Generic `tool call` — plná feature parity s MCP, pomalejší (subprocess overhead)

---

## Budoucí rozšíření (mimo scope tohoto phase)

1. **Persistent MCP daemon** — místo start/stop per call, běží na pozadí → eliminace 1-2s overhead
2. **kbagent jako MCP server** — `kbagent mcp serve` vystaví sebe jako MCP server, proxy k per-project serverům. AI klienti se připojí k jednomu MCP endpointu.
3. **Curated CLI příkazy** — `kbagent query "SQL"`, `kbagent run --component X` místo generického `tool call`
4. **Tool aliasy** — `kbagent tool call tables` místo `get_tables`
5. **Cachování tool listu** — tools jsou stejné pro všechny projekty, stačí fetchnout jednou
