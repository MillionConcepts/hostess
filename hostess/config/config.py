"""
desired settings should be placed in hostess/user_config/user_conifg.py.
this example_config module is never referenced by anything.
"""
import getpass
import os
from pathlib import Path

home = os.path.expanduser("~")

GENERAL_DEFAULTS = {
    "secrets_folders": (
        Path(home, ".hostess", "secrets"),
        Path(home),
        Path(home, "Downloads"),
        Path(home, ".ssh")
    ),
    "cache_path": f"{home}/.hostess/cache",
    "log_path": f"{home}/.hostess/logs",
    "uname": getpass.getuser(),
}
EC2_DEFAULTS = {
    "instance_type": "t3.small",
    "volume_type": "gp3",
    "volume_size": 8,
}
