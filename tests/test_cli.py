from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner

from qasync.cli import main
from qasync.syncer.base import SyncResult

SAMPLE_CONFIG = {
    "targets": {
        "local1": {
            "type": "local",
            "base_path": "/tmp/qasync-test-dest1",
            "rclone_remote": "local1",
        },
        "local2": {
            "type": "local",
            "base_path": "/tmp/qasync-test-dest2",
            "rclone_remote": "local2",
        },
    },
    "groups": {
        "both": ["local1", "local2"],
    },
    "defaults": {"parallel": 2},
}


@pytest.fixture
def config_file(tmp_path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text(yaml.dump(SAMPLE_CONFIG))
    return cfg


@pytest.fixture
def runner():
    return CliRunner()


def test_list_targets(runner, config_file):
    result = runner.invoke(main, ["list", "--config", str(config_file)])
    assert result.exit_code == 0
    assert "local1" in result.output
    assert "local2" in result.output
    assert "both" in result.output


def test_upload_missing_source(runner, config_file):
    result = runner.invoke(main, [
        "upload",
        "/nonexistent/path",
        "--targets",
        "local1",
        "--config",
        str(config_file),
    ])
    assert result.exit_code != 0
    assert "does not exist" in result.output


def test_upload_no_targets_or_group(runner, config_file, tmp_path):
    src = tmp_path / "data"
    src.mkdir()
    result = runner.invoke(main, [
        "upload",
        str(src),
        "--config",
        str(config_file),
    ])
    assert result.exit_code != 0


@patch("qasync.cli._check_rclone")
@patch("qasync.cli.run_sync")
def test_upload_with_targets(mock_run, mock_rclone, runner, config_file, tmp_path):
    src = tmp_path / "data"
    src.mkdir()
    mock_run.return_value = [
        SyncResult(target_name="local1", success=True, file_count=3, duration_seconds=0.5),
    ]
    result = runner.invoke(main, [
        "upload",
        str(src),
        "--targets",
        "local1",
        "--config",
        str(config_file),
    ])
    assert result.exit_code == 0
    assert "local1" in result.output
    assert "OK" in result.output


@patch("qasync.cli._check_rclone")
@patch("qasync.cli.run_sync")
def test_upload_with_group(mock_run, mock_rclone, runner, config_file, tmp_path):
    src = tmp_path / "data"
    src.mkdir()
    mock_run.return_value = [
        SyncResult(target_name="local1", success=True, file_count=3, duration_seconds=0.5),
        SyncResult(target_name="local2", success=True, file_count=3, duration_seconds=0.6),
    ]
    result = runner.invoke(main, [
        "upload",
        str(src),
        "--group",
        "both",
        "--config",
        str(config_file),
    ])
    assert result.exit_code == 0
    assert "local1" in result.output
    assert "local2" in result.output


def test_group_create(runner, config_file):
    result = runner.invoke(main, [
        "group",
        "create",
        "test-grp",
        "--targets",
        "local1",
        "--config",
        str(config_file),
    ])
    assert result.exit_code == 0
    assert "test-grp" in result.output


def test_group_delete(runner, config_file):
    result = runner.invoke(main, [
        "group",
        "delete",
        "both",
        "--config",
        str(config_file),
    ])
    assert result.exit_code == 0
    assert "deleted" in result.output


def test_remove_target(runner, config_file):
    result = runner.invoke(main, [
        "remove",
        "local1",
        "--config",
        str(config_file),
    ])
    assert result.exit_code == 0
    assert "removed" in result.output


@patch("qasync.setup.subprocess.run")
def test_add_s3_target_with_profile(mock_rclone_run, runner, tmp_path):
    """Test adding S3 target with profile auth."""
    mock_rclone_run.return_value = MagicMock(returncode=0, stderr="")
    config_file = tmp_path / "empty.yaml"
    # Input: backend=s3, bucket, base_path, region, auth=profile, profile=default
    result = runner.invoke(
        main,
        ["add", "my-s3", "--config", str(config_file)],
        input="s3\ntest-bucket\n/data\nus-west-2\nprofile\ndefault\n",
    )
    assert result.exit_code == 0
    assert "saved" in result.output
    cfg_text = config_file.read_text()
    assert "test-bucket" in cfg_text
    assert "us-west-2" in cfg_text


@patch("qasync.setup.subprocess.run")
def test_add_s3_target_with_access_key(mock_rclone_run, runner, tmp_path):
    """Test adding S3 target with access key auth."""
    mock_rclone_run.return_value = MagicMock(returncode=0, stderr="")
    config_file = tmp_path / "empty.yaml"
    # Input: backend=s3, bucket, base_path, region, auth=access-key, key, secret
    result = runner.invoke(
        main,
        ["add", "my-s3", "--config", str(config_file)],
        input="s3\ntest-bucket\n/\nus-east-1\naccess-key\nAKID123\nSECRET456\n",
    )
    assert result.exit_code == 0
    assert "saved" in result.output
    # Secret should NOT be in the qasync config (it's in rclone's config)
    cfg_text = config_file.read_text()
    assert "SECRET456" not in cfg_text


@patch("qasync.setup.subprocess.run")
def test_add_sftp_target_with_key(mock_rclone_run, runner, tmp_path):
    """Test adding SFTP target with SSH key auth."""
    mock_rclone_run.return_value = MagicMock(returncode=0, stderr="")
    config_file = tmp_path / "empty.yaml"
    # Input: backend=sftp, host, base_path, user, port, auth=ssh-key, key_file
    result = runner.invoke(
        main,
        ["add", "my-sftp", "--config", str(config_file)],
        input="sftp\nsftp.example.com\n/uploads\nqauser\n22\nssh-key\n~/.ssh/id_rsa\n",
    )
    assert result.exit_code == 0
    assert "saved" in result.output
