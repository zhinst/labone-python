"""Module for handling the result.capnp::Result struct.

This module provides a function to unwrap the result of a capnp call and
raise the corresponding exception if the call failed. It is intendet to
be used for low level capnp interface calls.
"""

import capnp

from labone.core.errors import LabOneCoreError, error_from_capnp
from labone.core.helper import CapnpStructReader


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
    except (capnp.KjException, AttributeError):
        pass
    try:
        raise error_from_capnp(result.err) from None
    except capnp.KjException:
        msg = f"Unable to parse Server response. Received: \n{result}"
        raise LabOneCoreError(msg) from None
