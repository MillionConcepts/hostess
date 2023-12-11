"""
desired settings should be placed in hostess/user_config/user_config.py.
"""
import os
from pathlib import Path

home = os.path.expanduser("~")

GENERAL_DEFAULTS = {
    "secrets_folders": (
        Path(home, ".hostess", "secrets"),
        Path(home),
        Path(home, "Downloads"),
        Path(home, ".ssh"),
    ),
    "cache_path": f"{home}/.hostess/cache",
    "log_path": f"{home}/.hostess/logs",
    "uname": "ubuntu",
}
S3_DEFAULTS = {
    "config": {
        "multipart_chunksize": 1024 * 1024 * 50,
        "multipart_threshold": 1024 * 1024 * 250,
    }
}
EC2_DEFAULTS = {
    "instance_type": "t3a.small",
    "volume_type": "gp3",
    "volume_size": 8,
}
