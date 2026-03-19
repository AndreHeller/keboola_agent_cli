"""KBC CLI integration service - wraps the kbc Go binary for LLM export.

Provides detection of the kbc binary and execution of 'kbc llm export'
with credentials auto-resolved from the config store.
"""

import logging
import re
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlparse

from ..config_store import ConfigStore
from ..constants import KBC_SUBPROCESS_TIMEOUT
from ..errors import ConfigError
from .base import BaseService

logger = logging.getLogger(__name__)


def detect_kbc_command() -> str | None:
    """Detect the kbc binary on PATH.

    Returns:
        The path to kbc binary, or None if not found.
    """
    return shutil.which("kbc")


def _extract_host_from_url(stack_url: str) -> str:
    """Extract hostname from a Keboola stack URL.

    Args:
        stack_url: Full URL like 'https://connection.keboola.com'.

    Returns:
        Hostname like 'connection.keboola.com'.

    Raises:
        ValueError: If the URL has no hostname.
    """
    hostname = urlparse(stack_url).hostname
    if not hostname:
        msg = f"Cannot extract hostname from URL: {stack_url}"
        raise ValueError(msg)
    return hostname


class KbcService(BaseService):
    """Business logic for kbc CLI integration.

    Wraps the kbc Go binary for operations that are not available
    through the Storage API or MCP server.
    """

    def __init__(self, config_store: ConfigStore) -> None:
        super().__init__(config_store=config_store)

    def check_kbc_available(self) -> dict:
        """Check if kbc binary is available (doctor-compatible format).

        Returns:
            Dict with check name, status, and message.
        """
        kbc_path = detect_kbc_command()
        if kbc_path is None:
            return {
                "check": "kbc_binary",
                "name": "kbc CLI",
                "status": "warn",
                "message": ("kbc binary not found on PATH. Install with: brew install keboola-cli"),
            }

        version = self.get_kbc_version()
        version_info = f" v{version}" if version else ""
        return {
            "check": "kbc_binary",
            "name": "kbc CLI",
            "status": "pass",
            "message": f"kbc{version_info} found at {kbc_path}",
        }

    @staticmethod
    def get_kbc_version() -> str | None:
        """Parse the kbc version from 'kbc --version' output.

        Returns:
            Version string like '2.44.0', or None if detection fails.
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

    def run_llm_export(
        self,
        alias: str | None = None,
        with_samples: bool = False,
        sample_limit: int | None = None,
        max_samples: int | None = None,
    ) -> int:
        """Run 'kbc llm export' for a specific project.

        Resolves credentials from config store, creates output directory,
        and streams kbc output directly to terminal.

        Args:
            alias: Project alias. If None, uses the single configured project.
            with_samples: Include data samples in export.
            sample_limit: Max rows per table sample.
            max_samples: Max number of tables to sample.

        Returns:
            Exit code from the kbc process.

        Raises:
            ConfigError: If kbc binary is not found or project not found.
        """
        kbc_path = detect_kbc_command()
        if kbc_path is None:
            raise ConfigError(
                "kbc binary not found on PATH. Install with: brew install keboola-cli"
            )

        # Resolve the single project
        projects = self.resolve_projects([alias] if alias else None)
        if len(projects) != 1:
            raise ConfigError(
                "LLM export requires exactly one project. "
                f"Use --project to specify one of: {', '.join(projects.keys())}"
            )

        resolved_alias, project = next(iter(projects.items()))
        host = _extract_host_from_url(project.stack_url)

        # Create output directory
        output_dir = Path.cwd() / resolved_alias
        output_dir.mkdir(parents=True, exist_ok=True)

        # Build command
        cmd = [
            kbc_path,
            "llm",
            "export",
            "--storage-api-host",
            host,
            "--storage-api-token",
            project.token,
            "--force",
            "--non-interactive",
            "--version-check=false",
            "--working-dir",
            str(output_dir),
        ]

        if with_samples:
            cmd.append("--with-samples")
            if sample_limit is not None:
                cmd.extend(["--sample-limit", str(sample_limit)])
            if max_samples is not None:
                cmd.extend(["--max-samples", str(max_samples)])

        logger.debug("Running kbc command: %s", " ".join(cmd[:6]) + " ...")

        # Stream directly to terminal (no capture)
        result = subprocess.run(cmd, check=False)
        return result.returncode
