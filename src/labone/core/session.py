"""Capnp session client."""
import uuid

import capnp

from labone.core.connection_layer import (
    KernelInfo,
    ServerInfo,
    create_session_client_stream,
)
from labone.core.resources import session_protocol_capnp  # type: ignore[attr-defined]


class Session:
    """Capnp session client.

    TODO document

    Args:
        connection: Asyncio stream connection to the server.
    """

    def __init__(
        self,
        connection: capnp.AsyncIoStream,
        kernel_info: KernelInfo,
        server_info: ServerInfo,
    ) -> None:
        self._client = capnp.TwoPartyClient(connection)
        self._kernel_info = kernel_info
        self._server_info = server_info
        self._session = self._client.bootstrap().cast_as(session_protocol_capnp.Session)
        # The client_id is required by most capnp messages to identify the client
        # on the server side. It is unique per session.
        self._client_id = uuid.uuid4()

    @staticmethod
    async def create(
        *,
        kernel_info: KernelInfo,
        server_info: ServerInfo,
    ) -> "Session":
        """Create a new session to a LabOne kernel.

        Since the creation of a new session happens asynchronously, this method
        is required, instead of a simple constructor (since a constructor can
        not be async).

        Warning: The initial socket creation and setup (handshake, ...) is
            currently not done asynchronously! The reason is that there is not
            easy way of doing this with the current capnp implementation.

        Args:
            kernel_info: Information about the target kernel.
            server_info: Information about the target data server.

        Returns:
            A new session to the specified kernel.

        Raises:
            KernelNotFoundError: If the kernel was not found.
            IllegalDeviceIdentifierError: If the device identifier is invalid.
            DeviceNotFoundError: If the device was not found.
            KernelLaunchFailureError: If the kernel could not be launched.
            FirmwareUpdateRequiredError: If the firmware of the device is outdated.
            InterfaceMismatchError: If the interface does not match the device.
            DifferentInterfaceInUseError: If the device is visible, but cannot be
                connected through the requested interface.
            DeviceInUseError: If the device is already in use.
            BadRequestError: If there is a generic problem interpreting the incoming
                request.
            LabOneConnectionError: If another error happens during the session creation.
        """
        sock, kernel_info_extended, server_info_extended = create_session_client_stream(
            kernel_info=kernel_info,
            server_info=server_info,
        )
        connection = await capnp.AsyncIoStream.create_connection(sock=sock)
        return Session(
            connection=connection,
            kernel_info=kernel_info_extended,
            server_info=server_info_extended,
        )

    @property
    def kernel_info(self) -> KernelInfo:
        """Information about the kernel."""
        return self._kernel_info

    @property
    def server_info(self) -> ServerInfo:
        """Information about the server."""
        return self._server_info
