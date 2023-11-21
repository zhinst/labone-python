"""Module for handling the result.capnp::Result struct.

This module provides a function to unwrap the result of a capnp call and
raise the corresponding exception if the call failed. It is intendet to
be used for low level capnp interface calls.
"""
import capnp

from labone.core import errors
from labone.core.helper import CapnpStructReader

_ZI_ERROR_MAP = {
    1: errors.CancelledError,
    3: errors.NotFoundError,
    4: errors.OverwhelmedError,
    5: errors.BadRequestError,
    6: errors.UnimplementedError,
    7: errors.InternalError,
    8: errors.UnavailableError,
    9: errors.LabOneTimeoutError,
}


def unwrap(result: CapnpStructReader) -> CapnpStructReader:
    """Unwrap a result.capnp::Result struct.

    Args:
        result: The result to be unwrapped.

    Returns:
        The unwrapped result.

    Raises:
    LabOneCancelledError: The request was cancelled.
    NotFoundError: The requested value or node was not found.
    OverwhelmedError: The server is overwhelmed.
    BadRequestError: The request could not be interpreted.
    UnimplementedError: The request is not implemented.
    InternalError: An internal error occurred.
    UnavailableError: The device is unavailable.
    LabOneTimeoutError: A timeout occurred on the server.
    """
    try:
        return result.ok
    except capnp.KjException:
        pass
    try:
        raise _ZI_ERROR_MAP.get(result.err.kind, errors.LabOneCoreError)(
            result.err.message,
            code=result.err.code,
            category=result.err.category,
        )
    except capnp.KjException as e:
        msg = f"Unable to parse Server response. Received: \n{result}"
        raise errors.LabOneCoreError(msg) from e
