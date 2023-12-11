import warnings
from pathlib import Path

from . import config

CONDA_DEFAULTS = config.CONDA_DEFAULTS
EC2_DEFAULTS = config.EC2_DEFAULTS
GENERAL_DEFAULTS = config.GENERAL_DEFAULTS

try:
    from . import user_config
except ImportError:
    user_config = None

for category, name in zip(
    (CONDA_DEFAULTS, EC2_DEFAULTS, GENERAL_DEFAULTS),
    ("CONDA_DEFAULTS", "EC2_DEFAULTS", "GENERAL_DEFAULTS")
):
    try:
        category |= getattr(user_config, name)
    except AttributeError:
        pass

for path in GENERAL_DEFAULTS["log_path"], GENERAL_DEFAULTS["cache_path"]:
    if not Path(path).exists():
        try:
            Path(path).mkdir(exist_ok=True, parents=True)
        except OSError:
            warnings.warn(f"{path} not accessible, logging/caching will fail")
