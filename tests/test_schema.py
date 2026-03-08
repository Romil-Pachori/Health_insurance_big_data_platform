from pathlib import Path

from src.utils.config import load_config


def test_config_env_resolution(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "test-host")
    config_file = tmp_path / "cfg.yaml"
    config_file.write_text("postgres:\n  host: ${POSTGRES_HOST:-localhost}\n", encoding="utf-8")
    config = load_config(str(config_file))
    assert config["postgres"]["host"] == "test-host"
