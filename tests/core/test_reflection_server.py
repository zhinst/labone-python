from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import capnp
import labone.core.reflection.server as reflection_server
import pytest
from labone.core.errors import LabOneCoreError


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
    create_from_connection.assert_called_once_with("connection", unwrap_result=True)


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
        unwrap_result=True,
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
    with pytest.raises(LabOneCoreError):
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
        unwrap_result=False,
    )
    assert server.capability_name == client.bootstrap().cast_as.return_value
    assert server.CapabilityName == dummy_schema
    parsed_schema.assert_called_once_with("encoded_schema")
    build_type_system.assert_called_once_with(
        parsed_schema.return_value.full_schema,
        server,
    )


@patch(
    "labone.core.reflection.server.ParsedWireSchema",
    autospec=True,
)
@patch(
    "labone.core.reflection.server.build_type_system",
    autospec=True,
)
def test_reflection_server_unwrap(build_type_system, parsed_schema):
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
        unwrap_result=True,
    )
    assert server.capability_name._capability == client.bootstrap().cast_as.return_value
    assert server.CapabilityName == dummy_schema
    parsed_schema.assert_called_once_with("encoded_schema")
    build_type_system.assert_called_once_with(
        parsed_schema.return_value.full_schema,
        server,
    )


def test_capability_wrapper_dir():
    fake_capability = MagicMock()
    wrapper = reflection_server.CapabilityWrapper(fake_capability)
    fake_capability.testResult = MagicMock()

    assert "testResult" in dir(wrapper)
    assert "test_result" in dir(wrapper)


def test_capability_wrapper_get_unkown_attr():
    fake_capability = MagicMock()
    wrapper = reflection_server.CapabilityWrapper(fake_capability)
    test = wrapper.setDummy
    assert test == fake_capability.setDummy


def test_capability_wrapper_get_request_unkown_attr():
    fake_capability = MagicMock()
    wrapper = reflection_server.CapabilityWrapper(fake_capability)
    test = wrapper.setDummy_request
    assert test == fake_capability.setDummy_request


@pytest.mark.asyncio()
async def test_capability_wrapper_get_send():
    fake_capability = MagicMock()
    wrapper = reflection_server.CapabilityWrapper(fake_capability)
    type(fake_capability.schema).method_names_inherited = PropertyMock(
        return_value=["setDummy"],
    )
    fake_capability._send = AsyncMock()
    test = await wrapper.setDummy(123)
    fake_capability._send.assert_awaited_once_with("setDummy", 123)
    assert test == fake_capability._send.return_value


@pytest.mark.asyncio()
async def test_capability_wrapper_get_send_snake_case():
    fake_capability = MagicMock()
    wrapper = reflection_server.CapabilityWrapper(fake_capability)
    type(fake_capability.schema).method_names_inherited = PropertyMock(
        return_value=["setDummy"],
    )
    fake_capability._send = AsyncMock()
    test = await wrapper.set_dummy(123)
    fake_capability._send.assert_awaited_once_with("setDummy", 123)
    assert test == fake_capability._send.return_value


def test_capability_wrapper_get_request():
    fake_capability = MagicMock()
    wrapper = reflection_server.CapabilityWrapper(fake_capability)
    type(fake_capability.schema).method_names_inherited = PropertyMock(
        return_value=["setDummy"],
    )
    test = wrapper.setDummy_request()
    assert isinstance(test, reflection_server.RequestWrapper)
    fake_capability._request.assert_called_once()


@pytest.mark.asyncio()
async def test_request_wrapper_send():
    fake_capnp_request = AsyncMock()
    wrapper = reflection_server.RequestWrapper(fake_capnp_request)
    await wrapper.send()
    fake_capnp_request.send.assert_called_once()


def test_request_wrapper_get():
    fake_capnp_request = MagicMock()
    wrapper = reflection_server.RequestWrapper(fake_capnp_request)
    assert wrapper.testValue == fake_capnp_request.testValue
    assert wrapper.test_value == fake_capnp_request.testValue


def test_request_wrapper_set():
    fake_capnp_request = MagicMock()
    wrapper = reflection_server.RequestWrapper(fake_capnp_request)
    wrapper.testValue = 1
    assert fake_capnp_request.testValue == 1
    wrapper.test_value = 1
    assert fake_capnp_request.testValue == 1


def test_request_wrapper_dir():
    fake_capnp_request = MagicMock()
    wrapper = reflection_server.RequestWrapper(fake_capnp_request)
    fake_capnp_request.testValue = MagicMock()

    assert "testValue" in dir(wrapper)
    assert "test_value" in dir(wrapper)


def test_maybe_wrap_interface_passthrough():
    maybe_capability = MagicMock()
    assert reflection_server._maybe_wrap_interface(maybe_capability) == maybe_capability


def test_maybe_wrap_interface_wrapped():
    maybe_capability = MagicMock(spec=capnp.lib.capnp._DynamicCapabilityClient)

    result = reflection_server._maybe_wrap_interface(maybe_capability)
    assert isinstance(result, reflection_server.CapabilityWrapper)


def test_maybe_unwrap_passthrough():
    maybe_result = MagicMock()
    assert reflection_server._maybe_unwrap(maybe_result)


def test_maybe_unwrap_unwrap():
    maybe_result = MagicMock()
    maybe_result.schema.fields_list = [MagicMock()]
    maybe_result.result = MagicMock()
    result = reflection_server._maybe_unwrap(maybe_result)
    assert result == maybe_result.result.ok
