"""
desired settings should be placed in killscreen/user_config/user_conifg.py.
this example_config module is never referenced by anything.
"""
import os
from pathlib import Path

home = os.path.expanduser("~")
DEFAULTS = {
    "uname": os.getlogin(),
    "secrets_folders": [
        Path(home, ".killscreen", "secrets"),
        Path(home, ".ssh"),
    ],
}
