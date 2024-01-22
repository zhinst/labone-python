"""Module for the low level communication (pre capnp) with a LabOne data server.

This module is able tro create a connection to a LabOne data server and perform
the necessary steps (e.g. handshake) to establish a connection.
"""
from __future__ import annotations

import contextlib
import typing as t

__all__ = [
    "KernelInfo",
    "DeviceKernelInfo",
    "ZIKernelInfo",
    "ServerInfo",
    "create_session_client_stream",
]

import json
import socket
from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from http import HTTPStatus
from http.client import HTTPConnection

import capnp
from packaging import version

from labone.core import errors

HelloMsgJson = t.Dict[str, str]


class KernelInfo(ABC):
    """Information about a LabOne kernel.

    Prototype class that defines the interface of a kernel info object.
    Needs to be derived from to be used.
    """

    @abstractmethod
    def with_capability_version(
        self,
        capability_version: version.Version | None,
    ) -> KernelInfo:
        """Create a new kernel info with a specific capability version.

        Args:
            capability_version: Capability version that should be used.

        Returns:
            New kernel info with the specified capability version.
        """

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the kernel."""

    @property
    @abstractmethod
    def capability_version(self) -> version.Version | None:
        """Name of the kernel."""

    @property
    @abstractmethod
    def identifier(self) -> str:
        """Identifier of the kernel in the upgrade request."""

    @property
    @abstractmethod
    def query(self) -> dict[str, str]:
        """Additional query that should be used in the upgrade request."""


class DeviceKernelInfo(KernelInfo):
    """Kernel info for a specific device kernel.

    Args:
        device_id: Identifier of the device. (e.g. dev1234)
        interface: Interface of the device. If empty the Data Server will
            automatically choose the right interface based on the available
            interfaces and a priority list. (default = "")
    """

    def __init__(
        self,
        device_id: str,
        interface: str = "",
        capability_version: version.Version | None = None,
    ) -> None:
        self._device_id = device_id
        self._interface = interface
        self._capability_version = capability_version

    def with_capability_version(
        self,
        capability_version: version.Version | None,
    ) -> KernelInfo:
        """Create a new kernel info with a specific capability version.

        Args:
            capability_version: Capability version that should be used.

        Returns:
            New kernel info with the specified capability version.
        """
        return DeviceKernelInfo(self._device_id, self._interface, capability_version)

    @property
    def name(self) -> str:
        """Name of the kernel."""
        return self._device_id

    @property
    def capability_version(self) -> version.Version | None:
        """Name of the kernel."""
        return self._capability_version

    @property
    def identifier(self) -> str:
        """Identifier of the kernel in the upgrade request."""
        return f"devid/{self._device_id}"

    @property
    def query(self) -> dict[str, str]:
        """Additional query that should be used in the upgrade request."""
        return {"interface": str(self._interface)}

    @property
    def device_id(self) -> str:
        """Identifier of the device."""
        return self._device_id

    @property
    def interface(self) -> str:
        """Interface of the device."""
        return self._interface


class ZIKernelInfo(KernelInfo):
    """Kernel info for the own kernel of the data server.

    This kernel serves the /zi nodes and lives in the data server itself.
    """

    def __init__(self, capability_version: version.Version | None = None) -> None:
        self._capability_version = capability_version

    def with_capability_version(
        self,
        capability_version: version.Version | None,
    ) -> KernelInfo:
        """Create a new kernel info with a specific capability version.

        Args:
            capability_version: Capability version that should be used.

        Returns:
            New kernel info with the specified capability version.
        """
        return ZIKernelInfo(capability_version)

    @property
    def name(self) -> str:
        """Name of the kernel."""
        return "zi"

    @property
    def capability_version(self) -> version.Version | None:
        """Name of the kernel."""
        return self._capability_version

    @property
    def identifier(self) -> str:
        """Identifier of the kernel in the upgrade request."""
        return "zi"

    @property
    def query(self) -> dict[str, str]:
        """Additional query that should be used in the upgrade request."""
        return {}


@dataclass(frozen=True)
class ServerInfo:
    """Information about a server."""

    host: str
    port: int
    hello_msg: HelloMsgJson | None = None


# The API level is used by LabOne to provide backwards compatibility.
# It is more or less deprecated in favour of the capability version.
# The HPK only supports the highest API level (6) and it will probably not be
# updated in the future.
API_LEVEL = 6

# The minimum capability version that is required by the labone api.
# 1.4.0 is the first version that supports the html interface.
MIN_ORCHESTRATOR_CAPABILITY_VERSION = version.Version("1.4.0")
# The latest known version of the orchestrator capability version.
TESTED_ORCHESTRATOR_CAPABILITY_VERSION = version.Version("1.6.0")
HELLO_MSG_FIXED_LENGTH = 256


def _open_socket(server_info: ServerInfo) -> socket.socket:
    """Open a plain socket to a server.

    Args:
        server_info: Information about the server to connect.

    Returns:
        Socket to the server.

    Raises:
        UnavailableError: If the connection to the server could not be established.
    """
    try:
        sock = socket.create_connection((server_info.host, server_info.port))
    except (ConnectionRefusedError, TimeoutError, socket.timeout, OSError) as e:
        msg = (
            "Unable to open connection to the data server at "
            f"{server_info.host}:{server_info.port}. "
            "Make sure that the server is running, host / port names are correct. "
            f"(Reason: {e})"
        )
        raise errors.UnavailableError(msg) from e
    return sock


def _construct_handshake_error_msg(host: str, port: int, info: str) -> str:
    """Construct a handshake error message.

    Since a lot of the message is shared between the different handshake errors,
    this function is used to construct the error message.

    Args:
        host: Hostname of the server.
        port: Port of the server.
        info: Additional information about the error but at the end of the
            error message.
    """
    return (
        f"Unable to open connection to the data server at {host}:{port}. "
        "This usually indicates a outdated LabOne version. "
        "Please update LabOne to the latest version. "
        f"Reason: {info}"
    )


def _client_handshake(
    sock: socket.socket,
    *,
    check: bool = True,
) -> HelloMsgJson:
    """Perform the zi client handshake with the server.

    The handshake is mandatory and is a fixed length json encoded string.

    The structure of the hello message is defined in the hello_msg.capnp
    schema.

    If the check flag is set to true, the hello message is checked for
    compatibility with the labone api. This ensures that the data server is
    compatible with the  current version of the labone api.

    Args:
        sock: Socket to the server.
        check: If true, the hello message is checked for compatibility with the
            labone api. If false, the hello message is returned without any
            checks.

    Returns:
        Received hello message.

    Raises:
        UnavailableError: If the server is not compatible with the labone api.
            (Only if `check` == True)
    """
    raw_hello_msg = sock.recv(HELLO_MSG_FIXED_LENGTH).rstrip(b"\x00")
    try:
        # The hello message is a json string, so we need to parse it with json
        # first and then convert it to a capnp message. This is due to the fact
        # that we want to keep the hello message as generic as possible.
        hello_msg: HelloMsgJson = json.loads(raw_hello_msg)
    except (json.JSONDecodeError, capnp.lib.capnp.KjException) as err:
        msg = _construct_handshake_error_msg(  # type: ignore [call-arg]
            *sock.getpeername(),
            f"Invalid JSON during Handshake: {raw_hello_msg.decode()}",
        )
        raise errors.UnavailableError(msg) from err
    if not check:
        return hello_msg
    if hello_msg.get("kind") != "orchestrator":  # type: ignore [attr-defined]
        msg = _construct_handshake_error_msg(  # type: ignore [call-arg]
            *sock.getpeername(),
            f"Invalid server kind: {hello_msg.get('kind')}",
        )
        raise errors.UnavailableError(msg)
    if hello_msg.get("protocol") != "http":
        msg = _construct_handshake_error_msg(  # type: ignore [call-arg]
            *sock.getpeername(),
            f" Invalid protocol: {hello_msg.get('protocol')}",
        )
        raise errors.UnavailableError(msg)

    try:
        capability_version = version.Version(hello_msg.get("schema", "0.0.0"))
    except version.InvalidVersion as err:
        msg = _construct_handshake_error_msg(  # type: ignore [call-arg]
            *sock.getpeername(),
            f"Unsupported LabOne Version: {hello_msg.get('l1Ver')}",
        )
        raise errors.UnavailableError(msg) from err
    if capability_version < MIN_ORCHESTRATOR_CAPABILITY_VERSION:
        msg = _construct_handshake_error_msg(  # type: ignore [call-arg]
            *sock.getpeername(),
            f"Unsupported LabOne Version: {hello_msg.get('l1Ver')}",
        )
        raise errors.UnavailableError(msg)
    if capability_version.major > TESTED_ORCHESTRATOR_CAPABILITY_VERSION.major:
        msg = str(
            "Unable to open connection to the data server at {host}:{port}. "
            "The server is using a newer LabOne version that is incompatible "
            "with the version of this api. Please update the latest version "
            "of the python package.",
        )
        raise errors.UnavailableError(msg)
    return hello_msg


def _raise_orchestrator_error(code: str, message: str) -> None:
    """Raise a labone orchestrator error based on the error message.

    Args:
        code: Error code of the error.
        message: Error message of the error.

    Raises:
        ValueError: If the error code is ok.
        UnavailableError: If the kernel was not found or unable to connect.
        BadRequestError: If there is a generic problem interpreting the incoming request
        InternalError: If the kernel could not be launched or another internal
            error occurred.
        LabOneCoreError: If the error can not be mapped to a known error.
    """
    if code == "kernelNotFound":
        raise errors.UnavailableError(message)
    if code == "illegalDeviceIdentifier":
        raise errors.BadRequestError(message)
    if code == "deviceNotFound":
        raise errors.UnavailableError(message)
    if code == "kernelLaunchFailure":
        raise errors.InternalError(message)
    if code == "firmwareUpdateRequired":
        raise errors.UnavailableError(message)
    if code == "interfaceMismatch":
        raise errors.UnavailableError(message)
    if code == "differentInterfaceInUse":
        raise errors.UnavailableError(message)
    if code == "deviceInUse":
        raise errors.UnavailableError(message)
    if code == "unsupportedApiLevel":
        raise errors.UnavailableError(message)
    if code == "badRequest":
        raise errors.BadRequestError(message)
    if code == "ok":
        msg = "Error expected but status code is ok"
        raise ValueError(msg)
    raise errors.LabOneCoreError(message)


def _raise_connection_error(
    response_status: int,
    response_info: HelloMsgJson | None,
) -> None:
    """Raises a connection error based on the http response.

    Args:
        response_status: Status code of the response.
        response_info: Result message from the server.

    Raises:
        UnavailableError: If the kernel was not found or unable to connect.
        BadRequestError: If there is a generic problem interpreting the incoming request
        InternalError: If the kernel could not be launched or another internal
            error occurred.
        LabOneCoreError: If the error can not be mapped to a known error.
    """
    with contextlib.suppress(
        capnp.KjException,
        AttributeError,
        KeyError,
        TypeError,
    ):
        _raise_orchestrator_error(
            response_info["err"].get("code", "unknown"),  # type: ignore [union-attr, index]
            response_info["err"].get("message", ""),  # type: ignore [union-attr, index]
        )
    # None existing or malformed result message raise generic error
    msg = (
        f"Unexpected HTTP error {HTTPStatus(response_status).name} ({response_status})"
    )
    raise errors.LabOneCoreError(msg)


def _http_get_info_request(
    sock: socket.socket,
    kernel_info: KernelInfo,
    extra_headers: dict[str, str] | None = None,
) -> tuple[int, KernelInfo, HelloMsgJson | None]:
    """Issue a HTTP get kernel info request to the server.

    This data server is expected to respond with a
    Result(KernelDescriptor, Error) message from the orchetstrator.capnp.

    Args:
        sock: Socket to the server.
        kernel_info: Information about the kernel to connect to.
        extra_headers: Additional headers that should be used in the request.

    Returns:
        Tuple of the status code, the updated KernelInfo and the KernelDescriptor.

    Raises:
        UnavailableError: If the kernel was not found or unable to connect.
    """
    host, port = sock.getpeername()
    connection = HTTPConnection(host, port)
    # Set the sock manually to prevent creating a new one
    connection.sock = sock
    headers = extra_headers if extra_headers else {}
    headers["Host"] = f"{host}:{port}"
    headers["Accept"] = "application/json"
    query = kernel_info.query
    query["apiLevel"] = str(API_LEVEL)
    query_str = "&".join([f"{k}={v}" for k, v in query.items()])
    url = f"/api/v1/kernel/{kernel_info.identifier}?{query_str}"

    connection.request(method="GET", url=url, headers=headers)
    response = connection.getresponse()
    # The response from the server is excepted to encoded with json
    # At least that what we specified with the `Accept` header.
    response_info = (
        json.loads(response.read().decode())
        if response.length and response.length > 0
        else None
    )
    if response.status >= HTTPStatus.MULTIPLE_CHOICES:
        _raise_connection_error(response.status, response_info)
    # Update the capability version of the kernel info
    capability_version_raw = response.headers.get("Zhinst-Kernel-Version", None)
    kernel_info_extended = kernel_info.with_capability_version(
        capability_version=version.Version(capability_version_raw)
        if capability_version_raw is not None
        else None,
    )
    return (
        response.status,
        kernel_info_extended,
        response_info,
    )


def _protocol_upgrade(
    sock: socket.socket,
    *,
    kernel_info: KernelInfo,
) -> KernelInfo:
    """Perform the protocol upgrade to the capnp protocol.

    Send a HTTP get request to the server to upgrade the protocol to capnp.

    Args:
        sock: Socket to the server.
        kernel_info: Information about the kernel to connect to.

    Returns:
        HTTP response from the server.

    Raises:
        UnavailableError: If the kernel was not found or unable to connect.
        BadRequestError: If there is a generic problem interpreting the incoming request
        InternalError: If the kernel could not be launched or another internal
            error occurred.
        LabOneCoreError: If the error can not be mapped to a known error.
    """
    response_status, kernel_info_extended, _ = _http_get_info_request(
        sock,
        kernel_info,
        {"Connection": "Upgrade", "Upgrade": "capnp"},
    )
    if response_status != HTTPStatus.SWITCHING_PROTOCOLS:
        # The upgrade was not performed
        msg = (
            f"Unable to connect to kernel {kernel_info.name}. "
            "The kernel is not not compatible with the LabOne API. "
            "Please update LabOne to the latest version."
        )
        raise errors.UnavailableError(
            msg,
        )
    return kernel_info_extended


def create_session_client_stream(
    *,
    kernel_info: KernelInfo,
    server_info: ServerInfo | None = None,
    sock: socket.socket | None = None,
    handshake: bool = True,
) -> tuple[socket.socket, KernelInfo, ServerInfo]:
    """Create a session client stream to a kernel.

    The stream is ready to use with capnp and the session protocol.

    Performs the following steps:
    1. Open a socket to the server (if `sock` == None).
    2. Perform the zi client handshake.
    3. Perform the protocol upgrade to capnp.

    Args:
        kernel_info: Required information about the kernel to connect to.
        server_info: Server info (default = None).
        sock: Existing socket to the server (default = None).
            If specified server_info will be ignored.
        handshake: If true, the zi handshake is performed (default = True).

    Returns:
        Tuple of the socket, the kernel info and the server info.
        The kernel info is updated with the capability version.
        The server info is updated with the hello message.

    Raises:
        ValueError: If both `sock` and `host` are specified.
        UnavailableError: If the kernel was not found or unable to connect.
        BadRequestError: If there is a generic problem interpreting the incoming request
        InternalError: If the kernel could not be launched or another internal
            error occurred.
        LabOneCoreError: If the error can not be mapped to a known error.
    """
    # The initialization of the connection is synchronous ...
    # This is due to the fact that capnp library does only support creating a connection
    # from a socket and not from a stream.
    if sock is None and server_info is not None:
        sock = _open_socket(server_info)
    elif server_info is not None:
        msg = "Either sock or server_info can be specified, not both."
        raise ValueError(msg)
    elif sock is None and server_info is None:
        msg = "Either sock or server_info must be specified."
        raise ValueError(msg)
    else:
        host, port = sock.getpeername()
        server_info = ServerInfo(host=host, port=port)
    if handshake:
        hello_msg = _client_handshake(sock)
        server_info_extended = replace(server_info, hello_msg=hello_msg)
    else:
        server_info_extended = server_info
    kernel_info_extended = _protocol_upgrade(sock, kernel_info=kernel_info)
    return sock, kernel_info_extended, server_info_extended
