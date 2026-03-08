import os
import re
from pathlib import Path
from typing import Any

import yaml

ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-(.*?))?\}")


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def resolve_path(path_value: str) -> Path:
    path = Path(path_value)
    return path if path.is_absolute() else project_root() / path


def _resolve_env_vars(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env_vars(v) for v in value]
    if isinstance(value, str):
        def replace(match: re.Match[str]) -> str:
            name = match.group(1)
            default = match.group(2) if match.group(2) is not None else ""
            return os.getenv(name, default)

        return ENV_PATTERN.sub(replace, value)
    return value


def load_config(config_path: str) -> dict[str, Any]:
    resolved = resolve_path(config_path)
    with resolved.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return _resolve_env_vars(config)


def run_date_or_default(config: dict[str, Any], run_date: str | None) -> str:
    if run_date:
        return run_date
    return config["project"]["default_run_date"]

