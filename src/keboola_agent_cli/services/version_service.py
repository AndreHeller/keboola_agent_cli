"""Version service - detect local versions and check for updates.

Provides version information for kbagent and its dependency:
- keboola-mcp-server - always runs latest via 'uvx keboola_mcp_server@latest',
  version resolved from PyPI
"""

import logging
import re
import shutil
from typing import Any

import httpx
from packaging.version import InvalidVersion, Version

from .. import __version__
from ..constants import (
    MCP_PYPI_URL,
    VERSION_CHECK_TIMEOUT,
)

logger = logging.getLogger(__name__)


def _is_uvx_available() -> bool:
    """Check if uvx is available on PATH."""
    return shutil.which("uvx") is not None


def _fetch_mcp_latest_version(timeout: float = VERSION_CHECK_TIMEOUT) -> str | None:
    """Fetch latest keboola-mcp-server version from PyPI.

    Args:
        timeout: HTTP request timeout in seconds.

    Returns:
        Version string like '1.46.0', or None on failure.
    """
    try:
        response = httpx.get(
            MCP_PYPI_URL,
            timeout=timeout,
            follow_redirects=True,
        )
        response.raise_for_status()
        data = response.json()
        version = data.get("info", {}).get("version", "")
        if re.match(r"\d+\.\d+\.\d+", version):
            return version
        return None
    except (httpx.HTTPError, KeyError, ValueError):
        logger.debug("Failed to fetch latest MCP server version", exc_info=True)
        return None


def _is_up_to_date(local: str | None, latest: str | None) -> bool | None:
    """Compare local and latest versions.

    Args:
        local: Locally installed version string.
        latest: Latest available version string.

    Returns:
        True if up to date, False if update available, None if comparison not possible.
    """
    if local is None or latest is None:
        return None
    try:
        return Version(local) >= Version(latest)
    except InvalidVersion:
        return None


class VersionService:
    """Business logic for version detection and update checks.

    Detects local versions of kbagent and checks for available
    keboola-mcp-server updates.
    """

    def get_versions(self) -> dict[str, Any]:
        """Get version information for kbagent and its dependency.

        keboola-mcp-server: always runs latest via 'uvx ... @latest',
        so we only need to check PyPI for the current latest version
        and whether uvx is available.

        Returns:
            Structured dict with kbagent version and dependency info.
        """
        uvx_available = _is_uvx_available()
        mcp_latest = _fetch_mcp_latest_version()

        mcp_entry: dict[str, Any] = {
            "name": "keboola-mcp-server",
            "description": "Keboola MCP Server (via uvx @latest)",
            "uvx_available": uvx_available,
            "latest_version": mcp_latest,
            "auto_updates": True,
        }

        return {
            "kbagent": {
                "version": __version__,
            },
            "dependencies": [
                mcp_entry,
            ],
        }
