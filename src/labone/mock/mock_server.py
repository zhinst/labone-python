"""Abstract reflection as base for mock servers.

A capnp reflection server is dynamically created from a binary schema file.
This server can provide the schema (getTheSchema) and takes
an additional server used for the actual functionality. This functionality
inserting allows for a abstract common reflection server.
"""

from __future__ import annotations

import socket
import typing as t
from abc import ABC
from typing import TYPE_CHECKING

import capnp

from labone.core.helper import ensure_capnp_event_loop
from labone.core.reflection.parsed_wire_schema import ParsedWireSchema
from labone.core.reflection.server import reflection_capnp

if TYPE_CHECKING:
    from pathlib import Path

    from capnp.lib.capnp import _CallContext, _DynamicStructBuilder, _InterfaceModule


class ServerTemplate(ABC):
    """Common interface for concrete server implementations.

    Both Hpk and Simplon servers will implement this interface.
    It stands for the actual functionality of the server, which
    will be defined in the subclasses.

    The id_ attribute stands for the unique capnp id of the
    concrete server.
    """

    id_: int


def capnp_server_factory(  # noqa: ANN201
    interface: _InterfaceModule,
    mock: ServerTemplate,
    schema_parsed_dict: dict[str, t.Any],
):
    """Dynamically create a capnp server.

    As a reflection schema is used, the concrete server interface
    is only known at runtime. This function is the
    at-runtime-approach to creating the concrete server.

    Args:
        interface: Capnp interface for the server.
        mock: The concrete server implementation.
        schema_parsed_dict: The parsed capnp schema as a dictionary.

    Returns:
        Dynamically created capnp server.
    """

    class MockServerImpl(interface.Server):
        """Dynamically created capnp server.

        Redirects all calls (except getTheSchema) to the concrete server implementation.
        """

        def __init__(self) -> None:
            self._mock = mock

        def __getattr__(
            self,
            name: str,
        ) -> _DynamicStructBuilder | list[_DynamicStructBuilder] | str | list[str]:
            """Redirecting all calls to the concrete server implementation."""
            if hasattr(self._mock, name):
                return getattr(self._mock, name)
            return getattr(super(), name)

        async def getTheSchema(  # noqa: N802
            self,
            _context: _CallContext,
            **kwargs,  # noqa: ARG002
        ) -> _DynamicStructBuilder:
            """Reflection: Capnp method to get the schema.

            Will be called by capnp as reaction to a getTheSchema request.
            Do not call this method directly.

            Args:
                _context: Capnp context.
                kwargs: Additional arguments.

            Returns:
                The parsed schema as a capnp object.
            """
            # Use `from_dict` to benefit from pycapnp lifetime management
            # Otherwise the underlying capnp object need to be copied manually to avoid
            # segfaults
            return _context.results.theSchema.from_dict(schema_parsed_dict)

    return MockServerImpl


class MockServer:
    """Abstracr reflection server.

    Takes in another server implementation defining the specific functionality.

    Args:
        capability_bytes: Path to the binary schema file.
        concrete_server: ServerTemplate with the actual functionality.

    Returns:
        A MockServer instance which can be started with `start`.

    Raises:
        FileNotFoundError: If the file does not exist.
        PermissionError: If the file cannot be read.
        capnp.lib.capnp.KjException: If the schema is invalid. Or the id
            of the concrete server is not in the schema.

    """

    def __init__(
        self,
        *,
        capability_bytes: Path,
        concrete_server: ServerTemplate,
    ):
        self._concrete_server = concrete_server
        with capability_bytes.open("rb") as f:
            schema_bytes = f.read()
        with reflection_capnp.CapSchema.from_bytes(schema_bytes) as schema:
            self._schema_parsed_dict = schema.to_dict()
            self._schema = ParsedWireSchema(schema.theSchema)
        self._capnp_interface = capnp.lib.capnp._InterfaceModule(  # noqa: SLF001
            self._schema.full_schema[concrete_server.id_].schema.as_interface(),
            self._schema.full_schema[concrete_server.id_].name,
        )
        self._server = None

    async def start(self) -> capnp.AsyncIoStream:
        """Starting the server and returning the client connection.

        Returns:
            The client connection.

        Raises:
            RuntimeError: If the server is already started.
        """
        if self._server is not None:  # pragma: no cover
            msg = "Server already started."  # pragma: no cover
            raise RuntimeError(msg)  # pragma: no cover
        await ensure_capnp_event_loop()
        # create local socket pair
        # Since there is only a single client there is no need to use a asyncio server
        read, write = socket.socketpair()
        reader = await capnp.AsyncIoStream.create_connection(sock=read)
        writer = await capnp.AsyncIoStream.create_connection(sock=write)
        # create server for the local socket pair
        self._server = capnp.TwoPartyServer(
            writer,
            bootstrap=capnp_server_factory(
                self._capnp_interface,
                self._concrete_server,
                self._schema_parsed_dict,
            )(),
        )
        return reader
