"""Shared constants for Keboola Agent CLI.

All magic numbers, default values, retry parameters, timeout settings,
and environment variable names are centralized here to avoid duplication
and ensure consistency across the codebase.
"""

import httpx

# --- HTTP Retry Constants ---
RETRYABLE_STATUS_CODES: set[int] = {429, 500, 502, 503, 504}
MAX_RETRIES: int = 3
BACKOFF_BASE: float = 1.0  # seconds; delays: 1s, 2s, 4s

# --- HTTP Timeout ---
DEFAULT_TIMEOUT: httpx.Timeout = httpx.Timeout(
    connect=5.0, read=30.0, write=10.0, pool=5.0
)

# --- API Error Handling ---
MAX_API_ERROR_LENGTH: int = 500

# --- Default Stack URL ---
DEFAULT_STACK_URL: str = "https://connection.keboola.com"

# --- Token Description ---
DEFAULT_TOKEN_DESCRIPTION: str = "kbagent-cli"

# --- Job Limits ---
DEFAULT_JOB_LIMIT: int = 50
MAX_JOB_LIMIT: int = 500

# --- Retry-After Header ---
MAX_RETRY_AFTER_SECONDS: int = 60

# --- MCP Timeouts ---
DEFAULT_MCP_TOOL_TIMEOUT: int = 60
DEFAULT_MCP_INIT_TIMEOUT: int = 30

# --- MCP Concurrency ---
# 0 = unlimited (all projects run in parallel); set KBAGENT_MCP_MAX_SESSIONS to throttle
DEFAULT_MCP_MAX_SESSIONS: int = 0

# --- Storage Job Polling ---
STORAGE_JOB_POLL_INTERVAL: float = 1.0  # seconds between polls
STORAGE_JOB_MAX_WAIT: float = 60.0  # max seconds to wait for a storage job

# --- Parallel Workers ---
MAX_PARALLEL_WORKERS_LIMIT: int = 100

# --- Environment Variable Names ---
ENV_MAX_PARALLEL_WORKERS: str = "KBAGENT_MAX_PARALLEL_WORKERS"
ENV_KBC_TOKEN: str = "KBC_TOKEN"
ENV_KBC_STORAGE_API_URL: str = "KBC_STORAGE_API_URL"
ENV_KBC_MANAGE_API_TOKEN: str = "KBC_MANAGE_API_TOKEN"
ENV_MCP_TOOL_TIMEOUT: str = "KBAGENT_MCP_TOOL_TIMEOUT"
ENV_MCP_INIT_TIMEOUT: str = "KBAGENT_MCP_INIT_TIMEOUT"
ENV_MCP_MAX_SESSIONS: str = "KBAGENT_MCP_MAX_SESSIONS"

# --- Version Check ---
VERSION_CHECK_TIMEOUT: float = 4.0  # seconds for fetching latest version from remote
KBC_SUBPROCESS_TIMEOUT: float = 5.0  # seconds for running kbc/mcp subprocess commands
KBC_GITHUB_RELEASES_URL: str = (
    "https://api.github.com/repos/keboola/keboola-as-code/releases/latest"
)
MCP_PYPI_URL: str = "https://pypi.org/pypi/keboola-mcp-server/json"

# --- Domain Validation Constants ---
VALID_COMPONENT_TYPES: list[str] = ["extractor", "writer", "transformation", "application"]
VALID_STATUSES: list[str] = ["processing", "terminated", "cancelled", "success", "error"]
