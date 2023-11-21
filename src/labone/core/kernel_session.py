"""Module for a session to a LabOne Kernel.

A Kernel is a remote server that provides access to a defined set of nodes.
It can be a device kernel that provides access to the device nodes but it
can also be a kernel that provides additional functionality, e.g. the
Data Server (ZI) kernel.

Every Kernel provides the same capnp interface and can therefore be handled
in the same way. The only difference is the set of nodes that are available
on the kernel.

The number of sessions to a kernel is not limited. However, due to the
asynchronous interface, it is often not necessary to have multiple sessions
to the same kernel.
"""
from __future__ import annotations

import capnp

from labone.core.connection_layer import (
    KernelInfo,
    ServerInfo,
    create_session_client_stream,
)
from labone.core.errors import LabOneCoreError, UnavailableError
from labone.core.helper import (
    ensure_capnp_event_loop,
)
from labone.core.reflection.server import ReflectionServer
from labone.core.session import Session


class KernelSession(Session):
    """Session to a LabOne kernel.

    Representation of a single session to a LabOne kernel. This class
    encapsulates the capnp interaction and exposes a Python native API.
    All functions are exposed as they are implemented in the kernel
    interface and are directly forwarded to the kernel through capnp.

    Each function implements the required error handling both for the
    capnp communication and the server errors. This means unless an Exception
    is raised the call was sucessfull.

    The KernelSession class is instantiated through the staticmethod
    `create()`.
    This is due to the fact that the instantiation is done asynchronously.
    To call the constructor directly an already existing capnp io stream
    must be provided.

    Example:
        >>> kernel_info = ZIKernelInfo()
        >>> server_info = ServerInfo(host="localhost", port=8004)
        >>> kernel_session = await KernelSession(
                kernel_info = kernel_info,
                server_info = server_info,
            )

    Args:
        reflection_server: The reflection server that is used for the session.
        kernel_info: Information about the target kernel.
        server_info: Information about the target data server.
    """

    def __init__(
        self,
        reflection_server: ReflectionServer,
        kernel_info: KernelInfo,
        server_info: ServerInfo,
    ) -> None:
        super().__init__(
            reflection_server.session,  # type: ignore[attr-defined]
            reflection_server=reflection_server,
        )
        self._kernel_info = kernel_info
        self._server_info = server_info

    @staticmethod
    async def create(
        *,
        kernel_info: KernelInfo,
        server_info: ServerInfo,
    ) -> KernelSession:
        """Create a new session to a LabOne kernel.

        Since the creation of a new session happens asynchronously, this method
        is required, instead of a simple constructor (since a constructor can
        not be asynchronous).

        Warning: The initial socket creation and setup (handshake, ...) is
            currently not done asynchronously! The reason is that there is not
            easy way of doing this with the current capnp implementation.

        Args:
            kernel_info: Information about the target kernel.
            server_info: Information about the target data server.

        Returns:
            A new session to the specified kernel.

        Raises:
            UnavailableError: If the kernel was not found or unable to connect.
            BadRequestError: If there is a generic problem interpreting the incoming
                request
            InternalError: If the kernel could not be launched or another internal
                error occurred.
            LabOneCoreError: If another error happens during the session creation.
        """
        sock, kernel_info_extended, server_info_extended = create_session_client_stream(
            kernel_info=kernel_info,
            server_info=server_info,
        )
        await ensure_capnp_event_loop()
        connection = await capnp.AsyncIoStream.create_connection(sock=sock)
        try:
            reflection_server = await ReflectionServer.create_from_connection(
                connection,
            )
        except LabOneCoreError as e:
            msg = str(
                f"Unable to connect to the server at ({server_info.host}:"
                f"{server_info.port}). Please update the LabOne software to the "
                f"latest version. (extended information: {e})",
            )
            raise UnavailableError(msg) from e

        return KernelSession(
            reflection_server=reflection_server,
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
