"""Basic dynamic reflection server.

This module implements a basic dynamic reflection server. It is used to
connected to a basic Zurich Instruments reflection server. Based on the
`reflection.capnp` schema, it loads all capabilities exposed by the server and
adds them as attributes to the server instance. This allows to access the
capabilities directly through the server instance.
"""
from __future__ import annotations

import asyncio
import logging
import re

import capnp

from labone.core.errors import LabOneConnectionError
from labone.core.helper import ensure_capnp_event_loop
from labone.core.reflection import (  # type: ignore[attr-defined, import-untyped]
    reflection_capnp,
)
from labone.core.reflection.capnp_dynamic_type_system import build_type_system
from labone.core.reflection.parsed_wire_schema import EncodedSchema, ParsedWireSchema

logger = logging.getLogger(__name__)

SNAKE_CASE_REGEX_1 = re.compile(r"(.)([A-Z][a-z]+)")
SNAKE_CASE_REGEX_2 = re.compile(r"([a-z0-9])([A-Z])")


def _to_snake_case(word: str) -> str:
    """Convert camel case to snake case.

    Args:
        word: Word to convert.

    Returns:
        Converted word.
    """
    s1 = SNAKE_CASE_REGEX_1.sub(r"\1_\2", word)
    return SNAKE_CASE_REGEX_2.sub(r"\1_\2", s1).lower()


async def _fetch_encoded_schema(
    client: capnp.TwoPartyClient,
) -> tuple[int, EncodedSchema]:
    """Fetch the encoded schema from the server.

    This is done through the reflection interface of the server.

    Args:
        client: Basic capnp client.

    Returns:
        The encoded schema and the id of the bootstrap capability.

    Raises:
        LabOneConnectionError: If the schema cannot be fetched from the server.
    """
    reflection = client.bootstrap().cast_as(reflection_capnp.Reflection)
    try:
        schema_and_bootstrap_cap = await reflection.getTheSchema()
    except capnp.lib.capnp.KjException as e:
        msg = str(
            "Unable to connect to the server. Could not fetch the schema "
            "from the server.",
        )
        raise LabOneConnectionError(msg) from e
    server_schema = schema_and_bootstrap_cap.theSchema.theSchema
    bootstrap_capability_id = schema_and_bootstrap_cap.theSchema.typeId
    return bootstrap_capability_id, server_schema


class ReflectionServer:
    """Basic dynamic reflection server.

    This class is used to connected to a basic Zurich Instruments reflection
    server. Based on the `reflection.capnp` schema, it loads all capabilites
    exposed by the server and adds them as attributes to the server instance.
    This allows to access the capabilities directly through the server instance.

    The ReflectionServer class is instantiated through the staticmethod
    `create()` or `create_from_connection`. This is due to the fact that the
    instantiation is done asynchronously.

    Args:
        connection: Raw capnp asyncio stream for the connection to the server.
        client: Basic capnp client.
        encoded_schema: The encoded schema of the server.
        bootstrap_capability_id: The id of the bootstrap capability.
    """

    def __init__(
        self,
        *,
        connection: capnp.AsyncIoStream,
        client: capnp.TwoPartyClient,
        encoded_schema: bytes,
        bootstrap_capability_id: int,
    ) -> None:
        self._connection = connection
        self._client = client
        self._parsed_schema = ParsedWireSchema(encoded_schema)
        build_type_system(self._parsed_schema.full_schema, self)

        # Add to the server an instance of the bootstrap capability.
        # So for example if the server exposes a FluxDevice interface,
        # server will have "flux_device" attribute.
        bootstrap_capability_name = self._parsed_schema.full_schema[
            bootstrap_capability_id
        ].name
        instance_name = _to_snake_case(bootstrap_capability_name)
        setattr(
            self,
            instance_name,
            self._client.bootstrap().cast_as(getattr(self, bootstrap_capability_name)),
        )

        logger.info(
            "Server exposes a %s interface. Access it with server. %s",
            bootstrap_capability_name,
            instance_name,
        )
        # Save the event loop the server was created in. This is needed to
        # close the rpc client connection in the destructor of the server.
        self._creation_loop = asyncio.get_event_loop()

    async def _close_rpc_client(self) -> None:  # pragma: no cover
        """Close the rpc client connection.

        This function is called in the destructor of the server. It closes the
        rpc client connection.

        There is a bit of a catch to this function. The capnp client does a lot
        of stuff in the background for every client. Before the server can be
        closed, the client needs to be closed. Python takes care of this
        automatically since the client is a member of the server.
        However the client MUST be closed in the same thread in which the kj
        event loop is running. If everything is done in the same thread, then
        there is not problem. However, if the kj event loop is running in a
        different thread, e.g. when using the sync wrapper, then the client
        needs to be closed in the same thread as the kj event loop. Thats why
        this function is async even though it does not need to be.
        """
        self._client.close()

    def __del__(self) -> None:  # pragma: no cover
        # call the close_rpc_client function in the event loop the server
        # was created in. See the docstring of the function for more details.
        if (
            hasattr(self, "_creation_loop")
            and self._creation_loop is not None
            and self._creation_loop.is_running()
            and asyncio.get_event_loop() != self._creation_loop
        ):
            _ = asyncio.ensure_future(
                self._close_rpc_client(),
                loop=self._creation_loop,
            )

    @staticmethod
    async def create(host: str, port: int) -> ReflectionServer:
        """Connect to a reflection server.

        Args:
            host: Host of the server.
            port: Port of the server.

        Returns:
            The reflection server instance.
        """
        await ensure_capnp_event_loop()
        connection = await capnp.AsyncIoStream.create_connection(host=host, port=port)
        return await ReflectionServer.create_from_connection(connection)

    @staticmethod
    async def create_from_connection(
        connection: capnp.AsyncIoStream,
    ) -> ReflectionServer:
        """Create a reflection server from an existing connection.

        Args:
            connection: Raw capnp asyncio stream for the connection to the server.

        Returns:
            The reflection server instance.
        """
        client = capnp.TwoPartyClient(connection)
        (
            bootstrap_capability_id,
            encoded_schema,
        ) = await _fetch_encoded_schema(client)
        return ReflectionServer(
            connection=connection,
            client=client,
            encoded_schema=encoded_schema,
            bootstrap_capability_id=bootstrap_capability_id,
        )
