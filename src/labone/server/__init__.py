"""Subpackage for the server functionality of the LabOne API.

Warning this module is deprecated and will be removed in the future. Use the
`zhinst-coms.server` and `labone.mock` module instead.
"""

from zhinst.comms.server import CapnpServer

from labone.mock.session import LabOneServerBase, MockSession, Subscription

__all__ = ["CapnpServer", "LabOneServerBase", "Subscription", "MockSession"]

import warnings

_deprecation_warning = (
    "The `labone.server` module is deprecated and will be removed in the future."
    " Use the `zhinst-coms.server` and `labone.mock` module instead."
)
warnings.warn(DeprecationWarning(_deprecation_warning), stacklevel=2)
