# qasync

QA test data sync tool -- fan-out uploads to multiple storage backends in a single command.

Wraps [rclone](https://rclone.org/) for the actual transfers, adding parallel fan-out, target groups, interactive setup, and real-time progress tracking.

## Supported Backends

| Backend | Type | Syncer |
|---------|------|--------|
| Amazon S3 | `s3` | rclone |
| Google Cloud Storage | `gcs` | rclone |
| Azure ADLS Gen2 | `azureblob` | rclone |
| HDFS | `hdfs` | PyArrow (native) |
| SFTP | `sftp` | rclone |
| FTP | `ftp` | rclone |
| Box | `box` | rclone |
| Dropbox | `dropbox` | rclone |
| Google Drive | `drive` | rclone |
| Local filesystem | `local` | rclone |

## Prerequisites

- Python 3.10+
- [rclone](https://rclone.org/install/)
- [uv](https://docs.astral.sh/uv/)

## Installation

### 1. Install prerequisites

```bash
# macOS
brew install rclone uv

# Linux (rclone)
curl https://rclone.org/install.sh | sudo bash

# Linux (uv)
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Clone and install

```bash
git clone https://github.com/aboueleyes-i/qasync.git
cd qasync

# Create venv and install (Python 3.10-3.13)
uv venv
uv pip install -e .

# If you have Python 3.14+, pin to 3.13:
# uv venv --python 3.13
# uv pip install -e .
```

### 3. Verify

```bash
uv run qasync --help
```

### Optional: HDFS support

```bash
uv pip install -e ".[hdfs]"
```

### Optional: global install

To make `qasync` available everywhere without `uv run`:

```bash
uv tool install -e .
qasync --help
```

## Quick Start

### Option A: Interactive wizard

```bash
qasync init
```

Walks you through adding multiple targets, verifies connectivity for each, then offers to create groups.

### Option B: Add targets one at a time

```bash
# Each command prompts for backend type, connection details, and credentials
qasync add my-s3
qasync add my-gcs
qasync add my-azure
```

The `add` command:
1. Asks for backend type and connection details (bucket, region, host, etc.)
2. Prompts for authentication (AWS profile/access key, service account, SSH key, etc.)
3. Creates the rclone remote automatically
4. Verifies connectivity and reports success or the exact error

### Create groups

```bash
qasync group create datalake --targets my-s3,my-gcs,my-azure
```

### Upload test data

```bash
# Upload to specific targets
qasync upload ./test-data --targets my-s3,my-gcs

# Upload to a group
qasync upload ./test-data --group datalake

# Upload to everything
qasync upload ./test-data --group all

# Preview first (no actual transfer)
qasync upload ./test-data --group all --dry-run

# Skip target/group flags -- interactive picker appears
qasync upload ./test-data
```

### Upload behavior

By default, `upload` creates a subdirectory on the remote named after the source directory:

```bash
qasync upload ./test-data --targets s3
# -> s3:bucket/base_path/test-data/file1.csv
# -> s3:bucket/base_path/test-data/subdir/file2.csv
```

Use `--flat` to upload contents directly into the base path (no subdirectory):

```bash
qasync upload ./test-data --targets s3 --flat
# -> s3:bucket/base_path/file1.csv
# -> s3:bucket/base_path/subdir/file2.csv
```

### Check connectivity

```bash
qasync check --targets my-s3,my-gcs

# Or skip flags for interactive picker
qasync check
```

Shows a table with status and error details for each target.

### Clean up

```bash
qasync clean /connector-tests --targets my-s3,my-gcs
```

### Retry on failure

If some targets fail during upload, qasync asks whether to retry:

```
2 target(s) failed: azure, sftp
Retry failed targets? [y/N]:
```

## CLI Reference

```
qasync init                                              # Interactive setup wizard
qasync upload <source> [--targets t1,t2 | --group name]  # Upload directory
qasync list                                              # Show targets and groups
qasync check  [--targets t1,t2 | --group name]           # Test connectivity
qasync clean  <remote-path> --targets t1,t2 | --group name
qasync add    <name>                                     # Add target interactively
qasync remove <name>                                     # Remove target + rclone remote
qasync group create <name> --targets t1,t2
qasync group delete <name>
```

When `--targets` and `--group` are both omitted (on commands that support it), an interactive target picker appears.

### Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--targets` | `-t` | -- | Comma-separated target names |
| `--group` | `-g` | -- | Target group name (`all` is built-in) |
| `--dry-run` | -- | off | Preview only, no transfers |
| `--flat` | -- | off | Upload contents directly into base_path (no subdirectory) |
| `--parallel` | `-p` | 3 | Max concurrent uploads |
| `--config` | `-c` | `~/.qasync/config.yaml` | Config file path |

## Configuration

Config lives at `~/.qasync/config.yaml`. Created automatically by `qasync add` or `qasync init`.

```yaml
targets:
  my-s3:
    type: s3
    bucket: qa-test-bucket
    base_path: /connector-tests
    rclone_remote: qa-my-s3
    region: us-east-1

  my-gcs:
    type: gcs
    bucket: qa-test-gcs
    base_path: /connector-tests
    rclone_remote: qa-my-gcs

  my-sftp:
    type: sftp
    host: sftp.qa.internal
    base_path: /upload/tests
    rclone_remote: qa-my-sftp

  my-hdfs:
    type: hdfs
    namenode: hdfs://namenode:8020
    base_path: /test-data

groups:
  datalake: [my-s3, my-gcs]
  transfer: [my-sftp]

defaults:
  parallel: 3
```

Secrets (API keys, tokens, passwords) are stored in rclone's own config (`~/.config/rclone/rclone.conf`), never in the qasync config. The qasync config is safe to share across the team.

## Authentication

`qasync add` handles credentials interactively per backend:

| Backend | Auth options |
|---------|-------------|
| S3 | AWS profile, access key/secret, env vars, IAM role |
| GCS | Service account JSON, Application Default Credentials |
| Azure | Storage account key, SAS token, env vars |
| SFTP | SSH key, password |
| FTP | Username/password (prompted in base setup) |
| Box, Dropbox, Google Drive | OAuth (browser flow via `rclone config reconnect`) |
| HDFS | Kerberos/Hadoop env (configured outside qasync) |

Credentials are passed directly to `rclone config create`. For OAuth backends (Box, Dropbox, Drive), `qasync add` creates the remote and then tells you to run `rclone config reconnect <remote>:` to complete the browser auth flow.

## How It Works

1. CLI resolves targets from `--targets`, `--group`, or the interactive picker
2. Each target gets a syncer instance (RcloneSyncer or HdfsSyncer)
3. Syncers run in parallel via `ThreadPoolExecutor` (capped by `--parallel`)
4. Each RcloneSyncer runs `rclone copy` as a subprocess, streaming stats for real-time progress
5. HdfsSyncer uses `pyarrow.fs.HadoopFileSystem` for native HDFS access
6. Progress is shown per-target with live percentage from rclone's transfer stats
7. Results are collected and printed as a rich table
8. If targets failed, offers to retry

```
             Sync Results
+---------+--------+-------+----------+
| Target  | Status | Files | Duration |
+---------+--------+-------+----------+
| my-gcs  | OK     |    42 |   14.1s  |
| my-s3   | OK     |    42 |   12.3s  |
+---------+--------+-------+----------+
```

Failures are per-target -- if Azure fails, S3 and GCS still complete.

## Development

```bash
# Install with dev dependencies
uv pip install -e ".[dev]"

# Run tests
uv run pytest tests/ -v

# Lint
uv run ruff check qasync/ tests/

# Type check
uv run ty check qasync/
```

## Project Structure

```
qasync/
  cli.py          # Click CLI entry point + init wizard
  config.py       # YAML config loading and target/group resolution
  runner.py       # Parallel fan-out orchestrator (rich progress bars)
  output.py       # Rich tables, interactive target picker, retry prompt
  setup.py        # Interactive "qasync add" prompts + auth per backend
  syncer/
    base.py       # BaseSyncer ABC + SyncResult dataclass
    rclone.py     # RcloneSyncer -- subprocess wrapper with progress streaming
    hdfs.py       # HdfsSyncer -- PyArrow native HDFS
    registry.py   # Backend type -> syncer class mapping
```

### Adding a new backend

1. Create `syncer/mybackend.py` implementing `BaseSyncer` (upload, clean, check)
2. Register it in `registry.py`
3. Add connection prompts to `setup.py` `BACKEND_PROMPTS`
4. If it needs custom auth, add an entry to `_AUTH_PROMPTS` in `setup.py`
