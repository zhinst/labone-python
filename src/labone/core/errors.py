"""LabOne Error classes for the core component of the API."""
from asyncio import QueueEmpty

from labone.errors import LabOneError


class LabOneCoreError(LabOneError):
    """Base class for all LabOne core errors."""


class LabOneTimeoutError(LabOneCoreError, TimeoutError):
    """Raised when a timeout occurs."""


class LabOneReadOnlyError(LabOneCoreError):
    """Raised when attempting to write a node that is read-only."""


class LabOneWriteOnlyError(LabOneCoreError):
    """Raised when attempting to read a node that is write-only."""


##################################################################
## Connection Errors                                            ##
##################################################################
class LabOneConnectionError(LabOneCoreError, ConnectionError):
    """Base class for all LabOne connection errors.

    Connection errors are raised when the connection to the data server
    cannot be established or is lost.
    """


class LabOneVersionMismatchError(LabOneConnectionError):
    """Raised when LabOne instance is not compatible with the client."""


class KernelNotFoundError(LabOneConnectionError):
    """Raised when the the specified kernel cannot be found.

    A Kernel is a specific server responsible for a specific device/interface
    """


class IllegalDeviceIdentifierError(LabOneConnectionError):
    """Raised when the device identifier is not valid."""


class DeviceNotFoundError(LabOneConnectionError):
    """Raised when the device cannot be found."""


class KernelLaunchFailureError(LabOneConnectionError):
    """Raised when the kernel cannot be launched."""


class FirmwareUpdateRequiredError(LabOneConnectionError):
    """Raised when the firmware of the device needs to be updated."""


class InterfaceMismatchError(LabOneConnectionError):
    """Raised if the interface of the device does not match the requested interface."""


class DifferentInterfaceInUseError(LabOneConnectionError):
    """Raised cannot be connected through the requested interface.

    This error is raised when the device is already connected through a different
    interface.
    """


class DeviceInUseError(LabOneConnectionError):
    """Raised when the device is already in use by a different server."""


class UnsupportedApiLevelError(LabOneConnectionError):
    """Raised when the API level of the device is not supported."""


class BadRequestError(LabOneConnectionError):
    """Raised when the request cannot be interpreted."""


##################################################################
## Streaming Errors                                             ##
##################################################################


class StreamingError(LabOneCoreError):
    """Base class for all LabOne streaming errors."""


class EmptyDisconnectedDataQueueError(StreamingError, QueueEmpty):
    """Raised when the data queue is empty and disconnected."""
