"""Version service - detect local versions and check for updates.

Provides version information for kbagent and its dependencies:
- kbc (Go CLI) - local version via subprocess, latest via GitHub API
- keboola-mcp-server - always runs latest via 'uvx keboola_mcp_server@latest',
  version resolved from PyPI
"""

import logging
import re
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx
from packaging.version import InvalidVersion, Version

from .. import __version__
from ..constants import (
    KBC_GITHUB_RELEASES_URL,
    KBC_SUBPROCESS_TIMEOUT,
    MCP_PYPI_URL,
    VERSION_CHECK_TIMEOUT,
)
from .kbc_service import detect_kbc_command

logger = logging.getLogger(__name__)


def _get_kbc_local_version() -> str | None:
    """Get locally installed kbc version by running 'kbc --version'.

    Returns:
        Version string like '2.44.0', or None if not installed/detectable.
    """
    kbc_path = detect_kbc_command()
    if kbc_path is None:
        return None

    try:
        result = subprocess.run(
            [kbc_path, "--version"],
            capture_output=True,
            text=True,
            timeout=KBC_SUBPROCESS_TIMEOUT,
            check=False,
        )
        match = re.search(r"(\d+\.\d+\.\d+)", result.stdout)
        return match.group(1) if match else None
    except (subprocess.TimeoutExpired, OSError):
        logger.debug("Failed to detect kbc version", exc_info=True)
        return None


def _is_uvx_available() -> bool:
    """Check if uvx is available on PATH."""
    return shutil.which("uvx") is not None


def _fetch_kbc_latest_version(timeout: float = VERSION_CHECK_TIMEOUT) -> str | None:
    """Fetch latest kbc version from GitHub releases API.

    Args:
        timeout: HTTP request timeout in seconds.

    Returns:
        Version string like '2.44.2', or None on failure.
    """
    try:
        response = httpx.get(
            KBC_GITHUB_RELEASES_URL,
            timeout=timeout,
            follow_redirects=True,
        )
        response.raise_for_status()
        data = response.json()
        tag = data.get("tag_name", "")
        # Strip 'v' prefix if present
        version = tag.lstrip("v")
        if re.match(r"\d+\.\d+\.\d+", version):
            return version
        return None
    except (httpx.HTTPError, KeyError, ValueError):
        logger.debug("Failed to fetch latest kbc version", exc_info=True)
        return None


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

    Detects local versions of kbagent dependencies and checks
    for available updates in parallel.
    """

    def get_versions(self) -> dict[str, Any]:
        """Get version information for kbagent and all dependencies.

        kbc: local install checked against GitHub releases.
        keboola-mcp-server: always runs latest via 'uvx ... @latest',
        so we only need to check PyPI for the current latest version
        and whether uvx is available.

        Returns:
            Structured dict with kbagent version and dependency info.
        """
        # Step 1: Detect local kbc version + check uvx availability
        kbc_local = _get_kbc_local_version()
        uvx_available = _is_uvx_available()

        # Step 2: Fetch latest versions in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            kbc_future = executor.submit(_fetch_kbc_latest_version)
            mcp_future = executor.submit(_fetch_mcp_latest_version)

            kbc_latest = kbc_future.result()
            mcp_latest = mcp_future.result()

        # Step 3: Build result
        # MCP server: runs via 'uvx keboola_mcp_server@latest' (always latest)
        # so there's no local vs remote mismatch -- just show availability
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
                {
                    "name": "kbc",
                    "description": "Keboola CLI (Go)",
                    "local_version": kbc_local,
                    "latest_version": kbc_latest,
                    "up_to_date": _is_up_to_date(kbc_local, kbc_latest),
                    "upgrade_command": "brew upgrade keboola-cli",
                },
                mcp_entry,
            ],
        }
