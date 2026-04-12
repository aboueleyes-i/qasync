import shutil
import subprocess

import pytest
import yaml
from click.testing import CliRunner

from qasync.cli import main


def rclone_available():
    return shutil.which("rclone") is not None


@pytest.mark.skipif(not rclone_available(), reason="rclone not installed")
class TestLocalIntegration:

    @pytest.fixture
    def setup(self, tmp_path):
        # Source directory with test files
        src = tmp_path / "source"
        src.mkdir()
        (src / "file1.csv").write_text("a,b,c\n1,2,3")
        (src / "subdir").mkdir()
        (src / "subdir" / "file2.parquet").write_bytes(b"\x00" * 100)

        # Destination directories
        dest1 = tmp_path / "dest1"
        dest2 = tmp_path / "dest2"
        dest1.mkdir()
        dest2.mkdir()

        # Create rclone remotes for local dirs
        subprocess.run(
            ["rclone", "config", "create", "qasync-test1", "local"],
            capture_output=True,
        )
        subprocess.run(
            ["rclone", "config", "create", "qasync-test2", "local"],
            capture_output=True,
        )

        # Config file
        config = {
            "targets": {
                "local1": {
                    "type": "local",
                    "base_path": str(dest1),
                    "rclone_remote": "qasync-test1",
                },
                "local2": {
                    "type": "local",
                    "base_path": str(dest2),
                    "rclone_remote": "qasync-test2",
                },
            },
            "groups": {"both": ["local1", "local2"]},
            "defaults": {"parallel": 2},
        }
        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml.dump(config))

        return src, dest1, dest2, config_file

    def test_upload_to_multiple_local_targets(self, setup):
        src, dest1, dest2, config_file = setup
        runner = CliRunner()
        result = runner.invoke(main, [
            "upload", str(src),
            "--group", "both",
            "--config", str(config_file),
        ])
        assert result.exit_code == 0
        assert "OK" in result.output

        # Verify files landed in both destinations (under source/ subdir)
        assert (dest1 / "source" / "file1.csv").exists()
        assert (dest1 / "source" / "subdir" / "file2.parquet").exists()
        assert (dest2 / "source" / "file1.csv").exists()
        assert (dest2 / "source" / "subdir" / "file2.parquet").exists()

    def test_dry_run_does_not_copy(self, setup):
        src, dest1, dest2, config_file = setup
        runner = CliRunner()
        result = runner.invoke(main, [
            "upload", str(src),
            "--targets", "local1",
            "--dry-run",
            "--config", str(config_file),
        ])
        assert result.exit_code == 0
        assert "DRY RUN" in result.output
        # No files should have been copied
        assert not (dest1 / "file1.csv").exists()

    def test_clean_removes_files(self, setup):
        src, dest1, dest2, config_file = setup
        runner = CliRunner()
        # First upload
        runner.invoke(main, [
            "upload", str(src),
            "--targets", "local1",
            "--config", str(config_file),
        ])
        assert (dest1 / "source" / "file1.csv").exists()

        # Then clean
        result = runner.invoke(main, [
            "clean", str(dest1),
            "--targets", "local1",
            "--config", str(config_file),
        ])
        assert result.exit_code == 0
        assert not dest1.exists()
