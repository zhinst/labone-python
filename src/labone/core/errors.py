"""LabOne Error classes for the core component of the API."""

from __future__ import annotations

from asyncio import QueueEmpty

from labone.errors import LabOneError


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
