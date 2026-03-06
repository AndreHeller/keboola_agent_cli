"""Tests for config directory resolution and ConfigStore source property."""

from pathlib import Path

import pytest

from keboola_agent_cli.config_store import ConfigStore, resolve_config_dir
from keboola_agent_cli.constants import LOCAL_CONFIG_DIR_NAME


class TestResolveConfigDir:
    """Tests for resolve_config_dir() priority chain."""

    def test_cli_flag_takes_precedence(self, tmp_path: Path) -> None:
        """CLI flag overrides everything."""
        custom_dir = tmp_path / "custom"
        custom_dir.mkdir()
        path, source = resolve_config_dir(cli_config_dir=str(custom_dir))
        assert path == custom_dir
        assert source == "cli-flag"

    def test_env_var_takes_precedence_over_local(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Env var overrides local .kbagent/ discovery."""
        # Create a local .kbagent/config.json in CWD
        local_dir = tmp_path / LOCAL_CONFIG_DIR_NAME
        local_dir.mkdir()
        (local_dir / "config.json").write_text("{}", encoding="utf-8")
        monkeypatch.chdir(tmp_path)

        env_dir = tmp_path / "env-config"
        env_dir.mkdir()
        monkeypatch.setenv("KBAGENT_CONFIG_DIR", str(env_dir))

        path, source = resolve_config_dir()
        assert path == env_dir
        assert source == "env-var"

    def test_walkup_finds_kbagent_in_cwd(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Walk-up finds .kbagent/config.json in CWD."""
        monkeypatch.delenv("KBAGENT_CONFIG_DIR", raising=False)
        local_dir = tmp_path / LOCAL_CONFIG_DIR_NAME
        local_dir.mkdir()
        (local_dir / "config.json").write_text("{}", encoding="utf-8")
        monkeypatch.chdir(tmp_path)

        path, source = resolve_config_dir()
        assert path == local_dir
        assert source == "local"

    def test_walkup_finds_kbagent_in_parent(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Walk-up finds .kbagent/config.json in parent directory."""
        monkeypatch.delenv("KBAGENT_CONFIG_DIR", raising=False)
        local_dir = tmp_path / LOCAL_CONFIG_DIR_NAME
        local_dir.mkdir()
        (local_dir / "config.json").write_text("{}", encoding="utf-8")

        child_dir = tmp_path / "subdir" / "deep"
        child_dir.mkdir(parents=True)
        monkeypatch.chdir(child_dir)

        # Patch Path.home to be above tmp_path so walk-up doesn't stop early
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path.parent))

        path, source = resolve_config_dir()
        assert path == local_dir
        assert source == "local"

    def test_walkup_stops_at_home(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Walk-up stops at $HOME and does not escape beyond it."""
        monkeypatch.delenv("KBAGENT_CONFIG_DIR", raising=False)
        # Place .kbagent/ ABOVE home
        home_dir = tmp_path / "home"
        home_dir.mkdir()
        work_dir = home_dir / "projects" / "myproject"
        work_dir.mkdir(parents=True)

        above_home = tmp_path / LOCAL_CONFIG_DIR_NAME
        above_home.mkdir()
        (above_home / "config.json").write_text("{}", encoding="utf-8")

        monkeypatch.chdir(work_dir)
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: home_dir))

        _path, source = resolve_config_dir()
        assert source == "global"  # Should NOT find the one above home

    def test_walkup_requires_config_json(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Walk-up requires config.json file, not just the directory."""
        monkeypatch.delenv("KBAGENT_CONFIG_DIR", raising=False)
        # Create .kbagent/ dir without config.json
        local_dir = tmp_path / LOCAL_CONFIG_DIR_NAME
        local_dir.mkdir()
        monkeypatch.chdir(tmp_path)

        _path, source = resolve_config_dir()
        assert source == "global"  # Empty dir should not match

    def test_falls_back_to_global(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Falls back to global when nothing found."""
        monkeypatch.delenv("KBAGENT_CONFIG_DIR", raising=False)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))

        path, source = resolve_config_dir()
        assert source == "global"
        assert "keboola-agent-cli" in str(path)

    def test_oserror_from_cwd_falls_back_to_global(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """OSError from Path.cwd() falls back to global."""
        monkeypatch.delenv("KBAGENT_CONFIG_DIR", raising=False)
        monkeypatch.setattr(Path, "cwd", classmethod(lambda cls: (_ for _ in ()).throw(OSError("no cwd"))))

        _path, source = resolve_config_dir()
        assert source == "global"


class TestConfigStoreSource:
    """Tests for ConfigStore.source property."""

    def test_default_source_is_global(self, tmp_path: Path) -> None:
        """Default source is 'global'."""
        store = ConfigStore(config_dir=tmp_path)
        assert store.source == "global"

    def test_custom_source(self, tmp_path: Path) -> None:
        """Source can be set via constructor."""
        store = ConfigStore(config_dir=tmp_path, source="local")
        assert store.source == "local"

    def test_cli_flag_source(self, tmp_path: Path) -> None:
        """Source can be 'cli-flag'."""
        store = ConfigStore(config_dir=tmp_path, source="cli-flag")
        assert store.source == "cli-flag"
