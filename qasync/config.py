from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


class ConfigError(Exception):
    pass


_TARGET_FIELDS = {
    "name", "type", "base_path", "bucket", "container",
    "host", "namenode", "rclone_remote",
}


@dataclass
class TargetConfig:
    name: str
    type: str
    base_path: str = ""
    bucket: str = ""
    container: str = ""
    host: str = ""
    namenode: str = ""
    rclone_remote: str = ""
    extras: dict = field(default_factory=dict)


@dataclass
class QaSyncConfig:
    targets: dict[str, TargetConfig] = field(default_factory=dict)
    groups: dict[str, list[str]] = field(default_factory=dict)
    defaults: dict = field(default_factory=dict)


DEFAULT_CONFIG_PATH = Path.home() / ".qasync" / "config.yaml"


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> QaSyncConfig:
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    return _parse_config(raw)


def load_config_from_dict(raw: dict) -> QaSyncConfig:
    return _parse_config(raw)


def _parse_config(raw: dict) -> QaSyncConfig:
    targets = {}
    for name, cfg in raw.get("targets", {}).items():
        known = {k: v for k, v in cfg.items() if k in _TARGET_FIELDS}
        extras = {k: v for k, v in cfg.items() if k not in _TARGET_FIELDS}
        targets[name] = TargetConfig(name=name, **known, extras=extras)
    return QaSyncConfig(
        targets=targets,
        groups=raw.get("groups", {}),
        defaults=raw.get("defaults", {}),
    )


def save_config(config: QaSyncConfig, path: Path = DEFAULT_CONFIG_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw: dict = {
        "targets": {},
        "groups": config.groups,
        "defaults": config.defaults,
    }
    for name, tc in config.targets.items():
        d = {k: v for k, v in tc.__dict__.items() if k not in ("name", "extras") and v}
        d.update(tc.extras)
        raw["targets"][name] = d
    with open(path, "w") as f:
        yaml.dump(raw, f, default_flow_style=False, sort_keys=False)


def resolve_targets(
    config: QaSyncConfig,
    target_names: Optional[list[str]] = None,
    group: Optional[str] = None,
) -> list[TargetConfig]:
    if target_names and group:
        raise ConfigError("Specify --targets or --group, not both")
    if group:
        if group == "all":
            return list(config.targets.values())
        if group not in config.groups:
            raise ConfigError(f"Unknown group: {group}")
        names = config.groups[group]
    elif target_names:
        names = target_names
    else:
        raise ConfigError("Specify --targets or --group")
    result = []
    for name in names:
        if name not in config.targets:
            raise ConfigError(f"Unknown target: {name}")
        result.append(config.targets[name])
    return result
