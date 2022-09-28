"""
desired settings should be placed in killscreen/user_config/user_conifg.py.
this example_config module is never referenced by anything.
"""
import getpass
import os
from pathlib import Path

home = os.path.expanduser("~")

GENERAL_DEFAULTS = {
    "secrets_folders": (
        Path(home, ".killscreen", "secrets"),
        Path(home),
        Path(home, "Downloads"),
        Path(home, ".ssh")
    ),
    "cache_path": f"{home}/.killscreen/cache",
    "log_path": f"{home}/.killscreen/logs",
    "uname": getpass.getuser(),
}
EC2_DEFAULTS = {
    "instance_type": "t3.small",
    "volume_type": "gp3",
    "volume_size": 8,
}
