#  Copyright (c) 2020 SBA - MIT License

from .selector import MockSelector, MockSocket, ListenSocket
try:
    from .version import version as __version__
except ImportError:
    # be conservative if version.py could not be generated
    # as it might happen on some CI platforms...
    __version__ = '0.0.0'
__all__ = [MockSocket, MockSelector, ListenSocket]
