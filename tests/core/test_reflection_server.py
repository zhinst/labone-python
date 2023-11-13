from unittest.mock import AsyncMock, MagicMock, patch

import capnp
import labone.core.reflection.server as reflection_server
import pytest
from labone.core.errors import LabOneConnectionError


@pytest.mark.asyncio()
@patch(
    "labone.core.reflection.server.ReflectionServer.create_from_connection",
    side_effect=AsyncMock(return_value="Test"),
    autospec=True,
)
@patch("labone.core.reflection.server.capnp", autospec=True)
async def test_create_host_port(capnp, create_from_connection):
    capnp.AsyncIoStream.create_connection = AsyncMock(return_value="connection")
    assert await reflection_server.ReflectionServer.create("host", 1234) == "Test"
    capnp.AsyncIoStream.create_connection.assert_called_once_with(
        host="host",
        port=1234,
    )
    create_from_connection.assert_called_once_with("connection")


@pytest.mark.asyncio()
@patch(
    "labone.core.reflection.server.ReflectionServer.__init__",
    autospec=True,
    return_value=None,
)
@patch(
    "labone.core.reflection.server._fetch_encoded_schema",
    side_effect=AsyncMock(
        return_value=("bootstrap_capability_id", "encoded_schema"),
    ),
    autospec=True,
)
@patch(
    "labone.core.reflection.server.capnp.TwoPartyClient",
    return_value="two_party_client",
)
async def test_create_from_connection(
    two_party_client,
    fetch_encoded_schema,
    init_server,
):
    created_server = await reflection_server.ReflectionServer.create_from_connection(
        "dummy_connection",
    )
    two_party_client.assert_called_once_with("dummy_connection")
    fetch_encoded_schema.assert_called_once_with("two_party_client")
    init_server.assert_called_once_with(
        created_server,
        connection="dummy_connection",
        client="two_party_client",
        encoded_schema="encoded_schema",
        bootstrap_capability_id="bootstrap_capability_id",
    )


@pytest.mark.asyncio()
async def test_fetch_encoded_schema_ok():
    client = MagicMock()
    schema = AsyncMock()
    client.bootstrap().cast_as.return_value.getTheSchema.side_effect = schema
    result = await reflection_server._fetch_encoded_schema(client)
    assert result == (
        schema.return_value.theSchema.typeId,
        schema.return_value.theSchema.theSchema,
    )


@pytest.mark.asyncio()
async def test_fetch_encoded_schema_err():
    client = MagicMock()
    client.bootstrap.return_value.cast_as.return_value.getTheSchema.side_effect = (
        capnp.lib.capnp.KjException("test")
    )
    with pytest.raises(LabOneConnectionError):
        await reflection_server._fetch_encoded_schema(client)


@patch(
    "labone.core.reflection.server.ParsedWireSchema",
    autospec=True,
)
@patch(
    "labone.core.reflection.server.build_type_system",
    autospec=True,
)
def test_reflection_server(build_type_system, parsed_schema):
    parsed_schema.return_value.full_schema.__getitem__().name = "CapabilityName"
    dummy_schema = MagicMock()
    build_type_system.side_effect = lambda _, server: setattr(
        server,
        "CapabilityName",
        dummy_schema,
    )
    client = MagicMock()
    server = reflection_server.ReflectionServer(
        connection="connection",
        client=client,
        encoded_schema="encoded_schema",
        bootstrap_capability_id="bootstrap_capability_id",
    )
    assert server.capability_name == client.bootstrap().cast_as.return_value
    assert server.CapabilityName == dummy_schema
    parsed_schema.assert_called_once_with("encoded_schema")
    build_type_system.assert_called_once_with(
        parsed_schema.return_value.full_schema,
        server,
    )
