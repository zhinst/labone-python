"""Abstract reflection as base for servers.

A capnp reflection server is dynamically created from a binary schema file.
This server can provide the schema (getTheSchema) and takes
an additional server used for the actual functionality. This functionality
inserting allows for a abstract common reflection server.
"""

from __future__ import annotations

import argparse
import asyncio
import socket
from abc import ABC
from contextlib import suppress
from typing import TYPE_CHECKING

import capnp

from labone.core.helper import CapnpStructReader, ensure_capnp_event_loop
from labone.core.reflection.parsed_wire_schema import ParsedWireSchema

if TYPE_CHECKING:
    from capnp.lib.capnp import _CallContext, _DynamicStructBuilder


class CapnpServer(ABC):
    """Common interface for concrete server implementations.

    Servers will implement this interface.
    It stands for the actual functionality of the server, which
    will be defined in the subclasses.

    The id_ attribute stands for the unique capnp id of the
    concrete server.
    """

    server_id: int
    type_id: int


def capnp_server_factory(
    stream: capnp.AsyncIoStream,
    schema: CapnpStructReader,
    server: CapnpServer,
) -> capnp.TwoPartyServer:
    """Dynamically create a capnp server.

    As a reflection schema is used, the concrete server interface
    is only known at runtime. This function is the
    at-runtime-approach to creating the concrete server.

    Args:
        stream: Stream for the server.
        schema: Parsed capnp schema (`reflection_capnp.CapSchema`).
        server: The concrete server implementation.

    Returns:
        Dynamically created capnp server.
    """
    schema_parsed_dict = schema.to_dict()
    parsed_schema = ParsedWireSchema(schema.theSchema)
    capnp_interface = capnp.lib.capnp._InterfaceModule(  # noqa: SLF001
        parsed_schema.full_schema[server.server_id].schema.as_interface(),
        parsed_schema.full_schema[server.server_id].name,
    )

    class ServerImpl(capnp_interface.Server):  # type: ignore[name-defined]
        """Dynamically created capnp server.

        Redirects all calls (except getTheSchema) to the concrete server implementation.
        """

        def __init__(self) -> None:
            self._server = server
            # parsed schema needs to stay alive as long as the server is.
            self._parsed_schema = parsed_schema

        def __getattr__(
            self,
            name: str,
        ) -> _DynamicStructBuilder | list[_DynamicStructBuilder] | str | list[str]:
            """Redirecting all calls to the concrete server implementation."""
            if hasattr(self._server, name):
                return getattr(self._server, name)
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
            _context.results.theSchema.from_dict(schema_parsed_dict)
            _context.results.theSchema.typeId = server.type_id

    return capnp.TwoPartyServer(stream, bootstrap=ServerImpl())


async def start_local_server(
    schema: CapnpStructReader,
    server: CapnpServer,
) -> tuple[capnp.TwoPartyServer, capnp.AsyncIoStream]:
    """Starting a local server.

    This is equivalent to the `capnp_server_factory` but with the addition that
    a local socket pair is created for the server.

    Args:
        schema: Parsed capnp schema (`reflection_capnp.CapSchema`).
        server: The concrete server implementation.

    Returns:
        The server and the client connection.
    """
    await ensure_capnp_event_loop()
    # create local socket pair
    # Since there is only a single client there is no need to use a asyncio server
    read, write = socket.socketpair()
    reader = await capnp.AsyncIoStream.create_connection(sock=read)
    writer = await capnp.AsyncIoStream.create_connection(sock=write)
    # create server for the local socket pair
    return capnp_server_factory(writer, schema, server), reader


def start_server(
    schema: CapnpStructReader,
    server: CapnpServer,
) -> None:
    """Start the server.

    Start the capnp server with the given schema and server instance.
    The server address and port can be specified via command line arguments.
    The server keeps running until it is interrupted.

    Args:
        schema: Parsed capnp schema (`reflection_capnp.CapSchema`).
        server: The concrete server implementation.
    """

    def _host_port() -> tuple[str, int]:
        parser = argparse.ArgumentParser(
            usage="""Runs the server bound to the given address/port ADDRESS. """,
        )

        parser.add_argument("address", help="ADDRESS:PORT")

        return parser.parse_args().address.split(":")

    async def _new_connection(
        stream: capnp.AsyncIoStream,
    ) -> None:
        await capnp_server_factory(
            stream=stream,
            schema=schema,
            server=server,
        ).on_disconnect()

    async def _run_server() -> None:
        await ensure_capnp_event_loop()
        host, port = _host_port()
        server = await capnp.AsyncIoStream.create_server(_new_connection, host, port)
        async with server:
            await server.serve_forever()

    with suppress(KeyboardInterrupt):
        asyncio.run(_run_server())
