from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qasync.syncer.hdfs import HdfsSyncer


@pytest.fixture
def hdfs_syncer():
    return HdfsSyncer("hdfs", {
        "type": "hdfs",
        "namenode": "hdfs://namenode:8020",
        "base_path": "/test-data",
    })


def test_upload_nonexistent_path(hdfs_syncer):
    result = hdfs_syncer.upload(Path("/nonexistent/path"))
    assert result.success is False
    assert "does not exist" in result.error


@patch("qasync.syncer.hdfs.HdfsSyncer._get_filesystem")
def test_upload_success(mock_get_fs, hdfs_syncer, tmp_path):
    test_dir = tmp_path / "data"
    test_dir.mkdir()
    (test_dir / "file1.csv").write_text("a,b,c")
    (test_dir / "sub").mkdir()
    (test_dir / "sub" / "file2.csv").write_text("d,e,f")

    mock_fs = MagicMock()
    mock_get_fs.return_value = mock_fs

    result = hdfs_syncer.upload(test_dir)
    assert result.success is True
    assert result.file_count == 2
    assert result.target_name == "hdfs"
    assert mock_fs.create_dir.called
    assert mock_fs.copy_file.call_count == 2


@patch("qasync.syncer.hdfs.HdfsSyncer._get_filesystem")
def test_upload_dry_run(mock_get_fs, hdfs_syncer, tmp_path):
    test_dir = tmp_path / "data"
    test_dir.mkdir()
    (test_dir / "file1.csv").write_text("a,b,c")

    result = hdfs_syncer.upload(test_dir, dry_run=True)
    assert result.success is True
    assert result.file_count == 1
    mock_get_fs.assert_not_called()


@patch("qasync.syncer.hdfs.HdfsSyncer._get_filesystem")
def test_upload_failure(mock_get_fs, hdfs_syncer, tmp_path):
    test_dir = tmp_path / "data"
    test_dir.mkdir()
    (test_dir / "file1.csv").write_text("a,b,c")

    mock_fs = MagicMock()
    mock_fs.copy_file.side_effect = OSError("connection refused")
    mock_get_fs.return_value = mock_fs

    result = hdfs_syncer.upload(test_dir)
    assert result.success is False
    assert "connection refused" in result.error


@patch("qasync.syncer.hdfs.HdfsSyncer._get_filesystem")
def test_check_success(mock_get_fs, hdfs_syncer):
    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = MagicMock()
    mock_get_fs.return_value = mock_fs
    reachable, error = hdfs_syncer.check()
    assert reachable is True
    assert error == ""


@patch("qasync.syncer.hdfs.HdfsSyncer._get_filesystem")
def test_check_failure(mock_get_fs, hdfs_syncer):
    mock_get_fs.side_effect = OSError("cannot connect")
    reachable, error = hdfs_syncer.check()
    assert reachable is False
    assert "cannot connect" in error
