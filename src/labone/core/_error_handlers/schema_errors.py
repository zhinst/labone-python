"""Error handler module for capnp schema errors.

This module provides helper functionality to convert errors
raised from invalid schema values.
"""
import capnp

from labone.core import errors


def convert_dynamic_schema_error(exception: Exception, msg_prefix: str = "") -> None:
    """Generic converter for dynamic `capnp` schema errors.

    Converts exceptions raised while assembling a schema into library errors.

    Args:
        exception: Exception occured while building the request.
        msg_prefix: Prefix for the exception message.

    Raises:
        LabOneCoreError: All errors are translated to `LabOneCoreError`.
    """
    # TODO(markush):  # noqa: FIX002, TD003
    # Format error messages to an understandable format.
    # Will probably require to take in the original request and field name
    # to access the schema.
    if isinstance(exception, capnp.lib.capnp.KjException):
        msg = msg_prefix + exception.description
        raise errors.LabOneCoreError(msg) from exception
    msg = msg_prefix + str(exception)
    raise errors.LabOneCoreError(msg) from exception
