from . import config

DEFAULTS = config.DEFAULTS

try:
    from . import user_config
    DEFAULTS |= user_config.DEFAULTS
except ImportError:
    pass
