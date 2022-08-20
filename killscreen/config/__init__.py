from . import config

EC2_DEFAULTS = config.EC2_DEFAULTS
GENERAL_DEFAULTS = config.GENERAL_DEFAULTS

try:
    from . import user_config
    for category, name in zip(
        (EC2_DEFAULTS, GENERAL_DEFAULTS),
        ("EC2_DEFAULTS", "GENERAL_DEFAULTS")
    ):
        try:
            category |= getattr(user_config, name)
        except AttributeError:
            pass
except ImportError:
    pass
