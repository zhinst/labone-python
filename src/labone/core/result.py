"""Module for handling the result.capnp::Result struct.

This module provides a function to unwrap the result of a capnp call and
raise the corresponding exception if the call failed. It is intendet to
be used for low level capnp interface calls.
"""
import capnp

from labone.core import errors
from labone.core.resources import (  # type: ignore[attr-defined]
    result_capnp,
)

# Mapping between the internal error codes and the corresponding LabOne
# exceptions. This list is intentional not complete. It only reflects the
# errors that are eventually raised by the API and relevant to treat
# differently from the generic LabOneCoreError.
# A complete list of all error codes can be found in the C API
# (ziAPI.h::ZIResult_enum)
_ZI_ERROR_MAP = {
    0x800C: errors.LabOneConnectionError,
    0x800D: errors.LabOneTimeoutError,
    0x8013: errors.LabOneReadOnlyError,
    0x8014: errors.KernelNotFoundError,
    0x8015: errors.DeviceInUseError,
    0x8016: errors.InterfaceMismatchError,
    0x8017: errors.LabOneTimeoutError,
    0x8018: errors.DifferentInterfaceInUseError,
    0x8019: errors.FirmwareUpdateRequiredError,
    0x801B: errors.DeviceNotFoundError,
    0x8020: errors.LabOneWriteOnlyError,
}


def unwrap(
    result: result_capnp.Result,
) -> capnp.lib.capnp._DynamicStructReader:  # noqa: SLF001
    """Unwrap a result.capnp::Result struct.

    Args:
        result: The result to be unwrapped.

    Returns:
        The unwrapped result.

    Raises:
        errors.LabOneConnectionError: If the connection is invalid.
        errors.LabOneTimeoutError: If the operation timed out.
        errors.LabOneReadOnlyError: If a write operation was attempted on a
            read-only node.
        errors.KernelNotFoundError: If the kernel cannot be found.
        errors.DeviceInUseError: If the device is already in use by a different
            server.
        errors.InterfaceMismatchError: If the interface of the device does not
            match the requested interface.
        errors.DifferentInterfaceInUseError: If the device is already connected
            through a different interface.
        errors.FirmwareUpdateRequiredError: If the firmware of the device needs
            to be updated before it can be used.
        errors.DeviceNotFoundError: If the device cannot be found.
        errors.LabOneWriteOnlyError: If a read operation was attempted on a
            write-only node.
        errors.LabOneCoreError: If non of the previous errors apply but the
            operation failed anyway. The message of the exception contains the
               a more detailed description of the error.
    """
    try:
        return result.ok
    except capnp.KjException:
        pass
    raise _ZI_ERROR_MAP.get(result.err.code, errors.LabOneCoreError)(result.err.message)
