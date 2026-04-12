import subprocess

import click

from qasync.config import _TARGET_FIELDS, QaSyncConfig, TargetConfig, save_config

# Base prompts per backend: (field_name, prompt_text, default_value)
# Fields in _TARGET_FIELDS go to TargetConfig, others go to extras + rclone config
BACKEND_PROMPTS = {
    "s3": [
        ("bucket", "S3 bucket name", ""),
        ("base_path", "Base path in bucket", "/"),
        ("region", "AWS region", "us-east-1"),
    ],
    "gcs": [
        ("bucket", "GCS bucket name", ""),
        ("base_path", "Base path in bucket", "/"),
    ],
    "azureblob": [
        ("container", "Azure container name", ""),
        ("base_path", "Base path in container", "/"),
        ("account", "Azure storage account name", ""),
    ],
    "sftp": [
        ("host", "SFTP hostname", ""),
        ("base_path", "Base path on server", "/"),
        ("user", "SSH username", ""),
        ("port", "SSH port", "22"),
    ],
    "ftp": [
        ("host", "FTP hostname", ""),
        ("base_path", "Base path on server", "/"),
        ("user", "FTP username", ""),
        ("port", "FTP port", "21"),
    ],
    "hdfs": [
        ("namenode", "HDFS namenode URI (e.g. hdfs://namenode:8020)", ""),
        ("base_path", "Base path in HDFS", "/"),
    ],
    "box": [
        ("base_path", "Base path in Box", "/"),
    ],
    "dropbox": [
        ("base_path", "Base path in Dropbox", "/"),
    ],
    "drive": [
        ("base_path", "Base path in Google Drive", "/"),
    ],
    "local": [
        ("base_path", "Local directory path", ""),
    ],
}

# rclone backends that need OAuth browser flow
_OAUTH_TYPES = {"box", "dropbox", "drive", "gcs"}

BACKEND_CHOICES = list(BACKEND_PROMPTS.keys())


def _prompt_s3_auth() -> dict:
    """Prompt for S3 authentication method. Returns rclone params."""
    auth_method = click.prompt(
        "Auth method",
        type=click.Choice(["profile", "access-key", "env", "iam-role"]),
        default="profile",
    )

    params = {"provider": "AWS"}

    if auth_method == "profile":
        profile = click.prompt("AWS profile name", default="default")
        params["profile"] = profile
        params["env_auth"] = "false"
    elif auth_method == "access-key":
        access_key = click.prompt("AWS Access Key ID")
        secret_key = click.prompt("AWS Secret Access Key", hide_input=True)
        params["access_key_id"] = access_key
        params["secret_access_key"] = secret_key
        params["env_auth"] = "false"
    elif auth_method == "env":
        # rclone reads AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from env
        params["env_auth"] = "true"
        click.echo("  Will use AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from environment.")
    elif auth_method == "iam-role":
        params["env_auth"] = "true"
        click.echo("  Will use IAM role attached to the instance.")

    return params


def _prompt_gcs_auth() -> dict:
    """Prompt for GCS authentication method. Returns rclone params."""
    auth_method = click.prompt(
        "Auth method",
        type=click.Choice(["service-account", "adc"]),
        default="adc",
    )

    params = {}

    if auth_method == "service-account":
        sa_file = click.prompt("Path to service account JSON file")
        params["service_account_file"] = sa_file
    elif auth_method == "adc":
        # Application Default Credentials
        click.echo("  Will use Application Default Credentials (gcloud auth).")

    return params


def _prompt_azure_auth() -> dict:
    """Prompt for Azure authentication method. Returns rclone params."""
    auth_method = click.prompt(
        "Auth method",
        type=click.Choice(["key", "sas-token", "env"]),
        default="key",
    )

    params = {}

    if auth_method == "key":
        key = click.prompt("Azure storage account key", hide_input=True)
        params["key"] = key
    elif auth_method == "sas-token":
        sas = click.prompt("SAS token (the ?sv=... part)")
        params["sas_url"] = sas
    elif auth_method == "env":
        params["env_auth"] = "true"
        click.echo("  Will use AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY from environment.")

    return params


def _prompt_sftp_auth() -> dict:
    """Prompt for SFTP authentication method. Returns rclone params."""
    auth_method = click.prompt(
        "Auth method",
        type=click.Choice(["password", "ssh-key"]),
        default="ssh-key",
    )

    params = {}

    if auth_method == "password":
        password = click.prompt("Password", hide_input=True)
        params["pass"] = password
    elif auth_method == "ssh-key":
        key_file = click.prompt(
            "Path to SSH private key",
            default="~/.ssh/id_rsa",
        )
        params["key_file"] = key_file

    return params


# Map backend type to auth prompt function
_AUTH_PROMPTS = {
    "s3": _prompt_s3_auth,
    "gcs": _prompt_gcs_auth,
    "azureblob": _prompt_azure_auth,
    "sftp": _prompt_sftp_auth,
}


def _run_rclone_config(
    remote_name: str, backend_type: str, rclone_params: dict
) -> bool:
    """Run rclone config create with the collected params. Returns True on success."""
    cmd = ["rclone", "config", "create", remote_name, backend_type]
    for k, v in rclone_params.items():
        if v:
            cmd.extend([k, str(v)])

    click.echo(f"\nCreating rclone remote '{remote_name}'...")

    # Show command but mask secrets
    safe_keys = {"secret_access_key", "pass", "key", "sas_url"}
    display_cmd = []
    skip_next = False
    for i, part in enumerate(cmd):
        if skip_next:
            display_cmd.append("****")
            skip_next = False
        elif part in safe_keys:
            display_cmd.append(part)
            skip_next = True
        else:
            display_cmd.append(part)
    click.echo(f"  Running: {' '.join(display_cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        click.echo(f"  Failed: {result.stderr.strip()}")
        return False

    click.echo(f"  Remote '{remote_name}' created.")

    # OAuth backends need interactive auth
    if backend_type in _OAUTH_TYPES:
        click.echo(f"\n  {backend_type} uses OAuth. To complete auth, run:")
        click.echo(f"    rclone config reconnect {remote_name}:")
        click.echo("  This will open a browser for you to authorize access.")

    return True


def add_target_interactive(config: QaSyncConfig, name: str, config_path) -> None:
    if name in config.targets:
        click.echo(
            f"Target '{name}' already exists. Use 'qasync remove {name}' first."
        )
        return

    click.echo(f"\nAdding target: {name}")
    backend_type = click.prompt(
        "Backend type",
        type=click.Choice(BACKEND_CHOICES),
    )

    # Collect base fields (bucket, path, host, etc.)
    prompts = BACKEND_PROMPTS.get(backend_type, [])
    target_fields = {"type": backend_type}
    extras = {}
    rclone_params = {}

    for field_name, prompt_text, default in prompts:
        value = click.prompt(prompt_text, default=default or None)
        if not value:
            continue
        if field_name in _TARGET_FIELDS:
            target_fields[field_name] = value
        else:
            extras[field_name] = value
            rclone_params[field_name] = value

    # Collect auth credentials
    if backend_type in _AUTH_PROMPTS:
        auth_params = _AUTH_PROMPTS[backend_type]()
        rclone_params.update(auth_params)

    # For non-HDFS backends, create rclone remote automatically
    if backend_type != "hdfs":
        remote_name = f"qa-{name}"
        target_fields["rclone_remote"] = remote_name
        _run_rclone_config(remote_name, backend_type, rclone_params)

    config.targets[name] = TargetConfig(name=name, extras=extras, **target_fields)
    save_config(config, config_path)
    click.echo(f"\nTarget '{name}' saved to config.")


def remove_target(config: QaSyncConfig, name: str, config_path) -> None:
    if name not in config.targets:
        click.echo(f"Target '{name}' not found.")
        return

    # Also remove the rclone remote
    rclone_remote = config.targets[name].rclone_remote
    if rclone_remote:
        click.echo(f"Removing rclone remote '{rclone_remote}'...")
        subprocess.run(
            ["rclone", "config", "delete", rclone_remote],
            capture_output=True,
            text=True,
        )

    del config.targets[name]
    # Remove from any groups
    for group_name, members in list(config.groups.items()):
        if name in members:
            members.remove(name)
    save_config(config, config_path)
    click.echo(f"Target '{name}' removed.")
