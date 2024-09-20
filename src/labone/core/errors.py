"""LabOne Error classes for the core component of the API."""

from __future__ import annotations

import typing as t
from asyncio import QueueEmpty

import zhinst.comms

from labone.core import hpk_schema
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


T = t.TypeVar("T")


def translate_comms_error(
    func: t.Callable[..., t.Awaitable[T]],
) -> t.Callable[..., t.Awaitable[T]]:
    """Translate zhinst.comms exceptions to labone exceptions.

    A decorator to catch all exceptions from zhinst.comms and re-raise
    them as LabOneCoreError exceptions.
    """

    def wrapper(*args, **kwargs) -> t.Awaitable[T]:
        try:
            return func(*args, **kwargs)
        except zhinst.comms.errors.CancelledError as e:
            raise LabOneCoreError(str(e)) from e
        except zhinst.comms.errors.NotFoundError as e:
            raise LabOneCoreError(str(e)) from e
        except zhinst.comms.errors.OverwhelmedError as e:
            raise LabOneCoreError(str(e)) from e
        except zhinst.comms.errors.BadRequestError as e:
            raise LabOneCoreError(str(e)) from e
        except zhinst.comms.errors.UnimplementedError as e:
            raise LabOneCoreError(str(e)) from e
        except zhinst.comms.errors.UnavailableError as e:
            raise UnavailableError(str(e)) from e
        except zhinst.comms.errors.TimeoutError as e:
            raise LabOneTimeoutError(str(e)) from e
        except zhinst.comms.errors.BaseError as e:
            raise LabOneCoreError(str(e)) from e

    return wrapper


def async_translate_comms_error(
    func: t.Callable[..., t.Awaitable[T]],
) -> t.Callable[..., t.Awaitable[T]]:
    """Translate zhinst.comms exceptions to labone exceptions.

    A decorator to catch all exceptions from zhinst.comms and re-raise
    them as LabOneCoreError exceptions.
    """

    async def wrapper(*args, **kwargs) -> T:
        try:
            return await func(*args, **kwargs)
        except zhinst.comms.errors.CancelledError as e:
            raise LabOneCoreError(str(e)) from e
        except zhinst.comms.errors.NotFoundError as e:
            raise LabOneCoreError(str(e)) from e
        except zhinst.comms.errors.OverwhelmedError as e:
            raise LabOneCoreError(str(e)) from e
        except zhinst.comms.errors.BadRequestError as e:
            raise LabOneCoreError(str(e)) from e
        except zhinst.comms.errors.UnimplementedError as e:
            raise LabOneCoreError(str(e)) from e
        except zhinst.comms.errors.UnavailableError as e:
            raise UnavailableError(str(e)) from e
        except zhinst.comms.errors.TimeoutError as e:
            raise LabOneTimeoutError(str(e)) from e
        except zhinst.comms.errors.BaseError as e:
            raise LabOneCoreError(str(e)) from e

    return wrapper


##################################################################
## Streaming Errors                                             ##
##################################################################


class StreamingError(LabOneCoreError):
    """Base class for all LabOne streaming errors."""


class EmptyDisconnectedDataQueueError(StreamingError, QueueEmpty):
    """Raised when the data queue is empty and disconnected."""


_ZI_ERROR_MAP = {
    hpk_schema.ErrorKind.cancelled: CancelledError,
    hpk_schema.ErrorKind.unknown: LabOneCoreError,
    hpk_schema.ErrorKind.notFound: NotFoundError,
    hpk_schema.ErrorKind.overwhelmed: OverwhelmedError,
    hpk_schema.ErrorKind.badRequest: BadRequestError,
    hpk_schema.ErrorKind.unimplemented: UnimplementedError,
    hpk_schema.ErrorKind.internal: InternalError,
    hpk_schema.ErrorKind.unavailable: UnavailableError,
    hpk_schema.ErrorKind.timeout: LabOneTimeoutError,
}


def raise_streaming_error(err: hpk_schema.Error) -> None:
    """Raise labone error from a labone error struct.

    Args:
        err: The streaming error to be converted.

    Raises:
        LabOneCoreError: The converted error.
    """
    raise _ZI_ERROR_MAP.get(err.kind, LabOneCoreError)(  # type: ignore[call-overload]
        err.message,
        code=err.code,
        category=err.category,
    )
