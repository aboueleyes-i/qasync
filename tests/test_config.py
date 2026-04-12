import pytest
import yaml

from qasync.config import (
    ConfigError,
    QaSyncConfig,
    TargetConfig,
    load_config,
    resolve_targets,
)

SAMPLE_CONFIG = {
    "targets": {
        "s3": {
            "type": "s3", "bucket": "test-bucket",
            "base_path": "/data", "rclone_remote": "qa-s3",
        },
        "gcs": {
            "type": "gcs", "bucket": "test-gcs",
            "base_path": "/data", "rclone_remote": "qa-gcs",
        },
        "hdfs": {"type": "hdfs", "namenode": "hdfs://nn:8020", "base_path": "/data"},
    },
    "groups": {
        "cloud": ["s3", "gcs"],
    },
    "defaults": {
        "parallel": 3,
    },
}


def _make_config(raw: dict = SAMPLE_CONFIG) -> QaSyncConfig:
    targets = {}
    for name, cfg in raw["targets"].items():
        targets[name] = TargetConfig(name=name, **cfg)
    return QaSyncConfig(
        targets=targets,
        groups=raw.get("groups", {}),
        defaults=raw.get("defaults", {}),
    )


def test_load_config_from_file(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump(SAMPLE_CONFIG))
    config = load_config(config_file)
    assert "s3" in config.targets
    assert "gcs" in config.targets
    assert config.defaults["parallel"] == 3


def test_load_config_missing_file(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "missing.yaml")


def test_resolve_targets_by_name():
    config = _make_config()
    targets = resolve_targets(config, target_names=["s3", "gcs"])
    assert len(targets) == 2
    assert targets[0].name == "s3"
    assert targets[1].name == "gcs"


def test_resolve_targets_by_group():
    config = _make_config()
    targets = resolve_targets(config, group="cloud")
    assert len(targets) == 2


def test_resolve_targets_builtin_all_group():
    config = _make_config()
    targets = resolve_targets(config, group="all")
    assert len(targets) == 3


def test_resolve_targets_unknown_name():
    config = _make_config()
    with pytest.raises(ConfigError, match="Unknown target"):
        resolve_targets(config, target_names=["nonexistent"])


def test_resolve_targets_unknown_group():
    config = _make_config()
    with pytest.raises(ConfigError, match="Unknown group"):
        resolve_targets(config, group="nonexistent")


def test_resolve_targets_no_args():
    config = _make_config()
    with pytest.raises(ConfigError, match="Specify"):
        resolve_targets(config)
