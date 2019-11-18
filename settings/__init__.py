import warnings

from .defaults import *  # noqa

try:
    from .locals import *  # noqa
except ImportError:
    warnings.warn(
        'No settings file found. Did you remember to '
        'copy local-dist.py to local.py?', ImportWarning,
    )
