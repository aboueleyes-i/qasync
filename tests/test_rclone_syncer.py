from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qasync.syncer.rclone import RcloneSyncer


@pytest.fixture
def s3_syncer():
    return RcloneSyncer("s3", {
        "type": "s3",
        "bucket": "test-bucket",
        "base_path": "/data",
        "rclone_remote": "qa-s3",
    })


def test_build_rclone_destination(s3_syncer):
    dest = s3_syncer._build_destination()
    assert dest == "qa-s3:test-bucket/data"


def test_build_rclone_destination_no_base_path():
    syncer = RcloneSyncer("s3", {
        "type": "s3",
        "bucket": "test-bucket",
        "base_path": "",
        "rclone_remote": "qa-s3",
    })
    assert syncer._build_destination() == "qa-s3:test-bucket"


def test_build_upload_command(s3_syncer):
    cmd = s3_syncer._build_upload_cmd(Path("/tmp/test-dir"), dry_run=False, subdir="test-dir")
    assert cmd[0:2] == ["rclone", "copy"]
    assert "/tmp/test-dir" in cmd
    assert cmd[-1] == "qa-s3:test-bucket/data/test-dir"


def test_build_upload_command_dry_run(s3_syncer):
    cmd = s3_syncer._build_upload_cmd(Path("/tmp/test-dir"), dry_run=True, subdir="test-dir")
    assert "--dry-run" in cmd
    assert cmd[-1] == "qa-s3:test-bucket/data/test-dir"


@patch("qasync.syncer.rclone.subprocess.run")
def test_upload_success(mock_run, s3_syncer, tmp_path):
    test_dir = tmp_path / "data"
    test_dir.mkdir()
    (test_dir / "file1.csv").write_text("a,b,c")
    (test_dir / "file2.csv").write_text("d,e,f")

    mock_run.return_value = MagicMock(returncode=0, stderr="")

    result = s3_syncer.upload(test_dir)
    assert result.success is True
    assert result.target_name == "s3"
    assert result.file_count == 2
    mock_run.assert_called_once()


@patch("qasync.syncer.rclone.subprocess.run")
def test_upload_failure(mock_run, s3_syncer, tmp_path):
    test_dir = tmp_path / "data"
    test_dir.mkdir()
    (test_dir / "file1.csv").write_text("a,b,c")

    mock_run.return_value = MagicMock(returncode=1, stderr="bucket not found")

    result = s3_syncer.upload(test_dir)
    assert result.success is False
    assert "bucket not found" in result.error


def test_upload_nonexistent_path(s3_syncer):
    result = s3_syncer.upload(Path("/nonexistent/path"))
    assert result.success is False
    assert "does not exist" in result.error


@patch("qasync.syncer.rclone.subprocess.run")
def test_check_success(mock_run, s3_syncer):
    mock_run.return_value = MagicMock(returncode=0, stderr="")
    reachable, error = s3_syncer.check()
    assert reachable is True
    assert error == ""


@patch("qasync.syncer.rclone.subprocess.run")
def test_check_failure(mock_run, s3_syncer):
    mock_run.return_value = MagicMock(
        returncode=1,
        stderr="2026/04/12 ERROR : bucket not found\n",
    )
    reachable, error = s3_syncer.check()
    assert reachable is False
    assert "bucket not found" in error


def test_sftp_destination():
    syncer = RcloneSyncer("sftp", {
        "type": "sftp",
        "host": "sftp.qa.internal",
        "base_path": "/upload/tests",
        "rclone_remote": "qa-sftp",
    })
    assert syncer._build_destination() == "qa-sftp:/upload/tests"
