"""LabOne Error classes for the core component of the API."""

from __future__ import annotations

import typing as t
from asyncio import QueueEmpty

from labone.errors import LabOneError

if t.TYPE_CHECKING:
    import capnp


class LabOneCoreError(LabOneError):
    """Base class for all LabOne core errors."""

    def __init__(self, message: str, code: int = 0x8000, category: str = ""):
        super().__init__(message)
        self.code = code
        self.category = category


class CancelledError(LabOneCoreError):
    """Raised when a Value or Node can not be found."""


class NotFoundError(LabOneCoreError):
    """Raised when a Value or Node can not be found."""


class OverwhelmedError(LabOneCoreError):
    """Raised when the server is overwhelmed."""


class BadRequestError(LabOneCoreError):
    """Raised when the request cannot be interpreted."""


class UnimplementedError(LabOneCoreError):
    """Raised when the request cannot be interpreted."""


class InternalError(LabOneCoreError):
    """Raised when an internal error occurs."""


class LabOneTimeoutError(LabOneCoreError, TimeoutError):
    """Raised when a timeout occurs."""


class UnavailableError(LabOneCoreError):
    """Raised when the kernel is unavailable."""


class SHFHeaderVersionNotSupportedError(LabOneCoreError):
    """Raised when the SHF header version is not supported."""

    def __init__(self, version: tuple[int, int]):
        msg = (
            f"The SHF extra header version {version[0]}.{version[1]} "
            "is not supported in this context."
        )
        super().__init__(msg, code=0, category="SHFHeaderVersionNotSupported")


##################################################################
## Streaming Errors                                             ##
##################################################################


class StreamingError(LabOneCoreError):
    """Base class for all LabOne streaming errors."""


class EmptyDisconnectedDataQueueError(StreamingError, QueueEmpty):
    """Raised when the data queue is empty and disconnected."""


_ZI_ERROR_MAP = {
    1: CancelledError,
    3: NotFoundError,
    4: OverwhelmedError,
    5: BadRequestError,
    6: UnimplementedError,
    7: InternalError,
    8: UnavailableError,
    9: LabOneTimeoutError,
}


def error_from_capnp(err: capnp.lib.capnp._DynamicStructReader) -> LabOneCoreError:
    """Create labone error from a error.capnp::Error struct.

    Args:
        err: The capnp error to be converted.

    Returns:
        The corresponding error.
    """
    return _ZI_ERROR_MAP.get(err.kind, LabOneCoreError)(
        err.message,
        code=err.code,
        category=err.category,
    )
