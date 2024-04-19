"""Tests for `labone.core.session.Session` functionality that requires a server."""

from __future__ import annotations

import asyncio
import json
import socket
import traceback
import typing
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from itertools import cycle
from typing import Any, Callable
from unittest.mock import AsyncMock, MagicMock

import capnp
import pytest
import pytest_asyncio

from labone.core import errors
from labone.core.connection_layer import ServerInfo, ZIKernelInfo
from labone.core.helper import request_field_type_description
from labone.core.kernel_session import KernelSession
from labone.core.reflection.server import ReflectionServer
from labone.core.session import (
    ListNodesFlags,
    ListNodesInfoFlags,
    Session,
    _send_and_wait_request,
)
from labone.core.subscription import DataQueue
from labone.core.value import AnnotatedValue
from labone.mock import AutomaticSessionFunctionality, spawn_hpk_mock
from labone.mock.entry_point import MockSession
from labone.mock.hpk_schema import get_schema
from labone.server.server import start_local_server
from labone.server.session import SessionInterface

from .resources import session_protocol_capnp, testfile_capnp, value_capnp


class SessionBootstrap(session_protocol_capnp.Session.Server):
    """A bootstrap of `labone.core.resource.session_protocol.Session` Server"""

    def __init__(self, mock):
        self._mock = mock

    async def listNodes(self, _context, **_):  # noqa: N802
        return self._mock.listNodes(_context.params, _context.results)

    async def listNodesJson(self, _context, **_):  # noqa: N802
        return self._mock.listNodesJson(_context.params, _context.results)

    async def setValue(self, _context, **_):  # noqa: N802
        return self._mock.setValue(_context.params, _context.results)

    async def getValue(self, _context, **_):  # noqa: N802
        return self._mock.getValue(_context.params, _context.results)

    async def subscribe(self, _context, **_):
        return self._mock.subscribe(_context.params, _context.results)


class CapnpServer:
    """A capnp server."""

    def __init__(self, connection: capnp.AsyncIoStream):
        self._connection = connection

    @property
    def connection(self) -> capnp.AsyncIoStream:
        """Connection to the server."""
        return self._connection

    @classmethod
    async def create(
        cls,
        obj: capnp.lib.capnp._DynamicCapabilityServer,
    ) -> CapnpServer:
        """Create a server for the given object."""
        read, write = socket.socketpair()
        write = await capnp.AsyncIoStream.create_connection(sock=write)
        _ = asyncio.create_task(cls._new_connection(write, obj))
        return cls(await capnp.AsyncIoStream.create_connection(sock=read))

    @staticmethod
    async def _new_connection(
        stream: capnp.AsyncIoStream,
        obj: capnp.lib.capnp._DynamicCapabilityServer,
    ):
        """Establish a new connection."""
        await capnp.TwoPartyServer(stream, bootstrap=obj).on_disconnect()


class DummyServer(typing.NamedTuple):
    session: KernelSession
    server: MagicMock


@pytest_asyncio.fixture()
async def mock_connection(reflection_server) -> tuple[KernelSession, MagicMock]:
    """Fixture for `labone.core.Session` and the server it is connected to.

    Returns:
        Session and a server mock, which is passed into `SessionBootstrap`.
    """
    mock_server = MagicMock()
    server = await CapnpServer.create(SessionBootstrap(mock_server))
    reflection = reflection_server
    client = capnp.TwoPartyClient(server.connection)
    reflection.session = client.bootstrap().cast_as(reflection.Session)

    session = KernelSession(
        reflection_server=reflection,
        kernel_info=ZIKernelInfo(),
        server_info=ServerInfo(host="localhost", port=8004),
    )
    return DummyServer(session=session, server=mock_server)


def test_session_with_unwrapping_reflection(reflection_server):
    reflection_server.session = MagicMock()

    session = Session(
        reflection_server.session,
        reflection_server=reflection_server,
    )

    assert session._session == reflection_server.session.capnp_capability


class TestSessionListNodes:
    @staticmethod
    def mock_return_value(val: list) -> Callable:
        def mock_method(_, results):
            results.paths = val

        return mock_method

    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        ("from_server", "from_api"),
        [
            (["foo", "bar"], ["foo", "bar"]),
            ([], []),
            ([""], [""]),
        ],
    )
    async def test_return_value(self, mock_connection, from_server, from_api):
        mock_connection.server.listNodes.side_effect = self.mock_return_value(
            from_server,
        )
        r = await mock_connection.session.list_nodes("path")
        assert r == from_api

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("path", [1, 1.1, ["a"], {"a": "b"}])
    async def test_invalid_path_type(self, mock_connection, path):
        with pytest.raises(TypeError):
            await mock_connection.session.list_nodes(path)

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("flags", list(ListNodesFlags))
    async def test_with_flags_enum(self, mock_connection, flags):
        mock_connection.server.listNodes.side_effect = self.mock_return_value([])
        r = await mock_connection.session.list_nodes("path", flags=flags)
        assert r == []

    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        "flags",
        [0, 46378, 983, 354, 44, 10000],
    )
    async def test_with_flags_int(self, mock_connection, flags):
        mock_connection.server.listNodes.side_effect = self.mock_return_value([])
        r = await mock_connection.session.list_nodes("path", flags=flags)
        assert r == []

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("flags", ["foo", [3], None])
    async def test_with_flags_type_error(self, mock_connection, flags):
        with pytest.raises(TypeError):
            await mock_connection.session.list_nodes("path", flags=flags)

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("flags", [-2, -100])
    async def test_with_flags_value_error(self, mock_connection, flags):
        with pytest.raises(ValueError):
            await mock_connection.session.list_nodes("path", flags=flags)


class TestSessionListNodesJson:
    @staticmethod
    def mock_return_value(val: dict) -> Callable:
        def mock_method(_, results):
            results.nodeProps = json.dumps(val)

        return mock_method

    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        ("from_server", "from_api"),
        [
            ({"foo": "bar", "bar": "foo"}, {"foo": "bar", "bar": "foo"}),
            ({}, {}),
        ],
    )
    async def test_return_value(self, mock_connection, from_server, from_api):
        mock_connection.server.listNodesJson.side_effect = self.mock_return_value(
            from_server,
        )
        r = await mock_connection.session.list_nodes_info("path")
        assert r == from_api

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("path", [1, 1.1, ["a"], {"a": "b"}])
    async def test_invalid_path_type(self, mock_connection, path):
        with pytest.raises(TypeError):
            await mock_connection.session.list_nodes_info(path)

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("flags", list(ListNodesInfoFlags))
    async def test_with_flags_enum(self, mock_connection, flags):
        mock_connection.server.listNodesJson.side_effect = self.mock_return_value({})
        r = await mock_connection.session.list_nodes_info("path", flags=flags)
        assert r == {}

    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        "flags",
        [0, 46378, 983, 354, 44, 10000],
    )
    async def test_with_flags_int(self, mock_connection, flags):
        mock_connection.server.listNodesJson.side_effect = self.mock_return_value({})
        r = await mock_connection.session.list_nodes_info("path", flags=flags)
        assert r == {}
        mock_connection.server.listNodesJson.assert_called_once()

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("flags", ["foo", [3], None])
    async def test_with_flags_type_error(self, mock_connection, flags):
        with pytest.raises(TypeError):
            await mock_connection.session.list_nodes_info("path", flags=flags)

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("flags", [-2, -100])
    async def test_with_flags_value_error(self, mock_connection, flags):
        with pytest.raises(ValueError):
            await mock_connection.session.list_nodes_info("path", flags=flags)


async def mock_remote_response(response):
    """Simple function that returns a promise that resolves to `response`."""
    return response


async def mock_remote_error(error):
    """Simple function that returns a promise that rejects with `error`."""
    raise error


class MockRequest:
    """Mock of `capnp.lib.capnp._Request`"""

    def __init__(
        self,
        send_response: Any = None,
        send_raise_for: Any = None,
    ):
        self._send_response = send_response
        self._send_raise_for = send_raise_for

    def send(self):
        if self._send_raise_for:
            raise self._send_raise_for
        return self._send_response


class TestSendAndWaitRequest:
    @pytest.mark.asyncio()
    async def test_success(self):
        promise = mock_remote_response("foobar")
        response = await _send_and_wait_request(MockRequest(promise))
        assert response == "foobar"

    @pytest.mark.asyncio()
    async def test_send_kj_error(self):
        with pytest.raises(errors.LabOneCoreError, match="error"):
            await _send_and_wait_request(
                MockRequest(send_raise_for=capnp.lib.capnp.KjException("error")),
            )

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("error", [RuntimeError, AttributeError, ValueError])
    async def test_send_misc_error(self, error):
        with pytest.raises(errors.LabOneCoreError, match="error"):
            await _send_and_wait_request(
                MockRequest(send_raise_for=error("error")),
            )

    @pytest.mark.asyncio()
    async def test_suppress_unwanted_traceback(self):
        # Flaky test..
        try:
            await _send_and_wait_request(
                MockRequest(send_raise_for=capnp.lib.capnp.KjException("error")),
            )
        except errors.LabOneCoreError:
            assert "KjException" not in traceback.format_exc()

        promise = mock_remote_error(RuntimeError("error"))
        try:
            await _send_and_wait_request(MockRequest(promise))
        except errors.LabOneCoreError:
            assert "RuntimeError" not in traceback.format_exc()

    @pytest.mark.asyncio()
    async def test_a_wait_kj_error(self):
        promise = mock_remote_error(capnp.lib.capnp.KjException("error"))
        with pytest.raises(errors.LabOneCoreError, match="error"):
            await _send_and_wait_request(MockRequest(promise))

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("error", [RuntimeError, AttributeError, ValueError])
    async def test_a_wait_misc_error(self, error):
        promise = mock_remote_error(error("error"))
        with pytest.raises(errors.LabOneCoreError, match="error"):
            await _send_and_wait_request(MockRequest(promise))


@pytest.mark.asyncio()
async def test_capnprequest_field_type_description():
    class TestInterface(testfile_capnp.TestInterface.Server):
        pass

    client = testfile_capnp.TestInterface._new_client(TestInterface())
    request = client.testMethod_request()
    assert request_field_type_description(request, "testUint32Field") == "uint32"
    assert request_field_type_description(request, "testTextField") == "text"


def session_proto_value_to_python(builder):
    """`labone.core.resources.session_protocol_capnp:Value` to a Python value."""
    return getattr(builder, builder.which())


@dataclass
class ServerRecords:
    params: list[Any] = field(default_factory=list)


class TestSetValue:
    """Integration tests for Session node set values functionality."""

    @pytest.mark.asyncio()
    async def test_server_receives_correct_value(self, mock_connection):
        recorder = ServerRecords()

        def mock_method(params, _):
            param_builder = params.as_builder()
            recorder.params.append(param_builder)

        mock_connection.server.setValue.side_effect = mock_method

        value = AnnotatedValue(value=12, path="/foo/bar")
        with pytest.raises(errors.LabOneCoreError):
            await mock_connection.session.set(value)
        assert len(recorder.params) == 1
        assert recorder.params[0].pathExpression == "/foo/bar"
        assert session_proto_value_to_python(recorder.params[0].value) == 12

    @pytest.mark.asyncio()
    async def test_server_response_ok(self, mock_connection, reflection_server):
        value = AnnotatedValue(value=123, path="/bar/foobar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 1)
            builder[0].ok = value.to_capnp(reflection=reflection_server)

        mock_connection.server.setValue.side_effect = mock_method
        response = await mock_connection.session.set(value)
        assert response == value

    @pytest.mark.asyncio()
    async def test_server_response_err_single(self, mock_connection):
        value = AnnotatedValue(value=123, path="/bar/foobar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 1)
            builder[0].from_dict({"err": {"code": 1, "message": "test error"}})

        mock_connection.server.setValue.side_effect = mock_method

        with pytest.raises(errors.LabOneCoreError, match="test error"):
            await mock_connection.session.set(value)

    @pytest.mark.asyncio()
    async def test_server_response_err_multiple(self, mock_connection):
        value = AnnotatedValue(value=123, path="/bar/foobar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 2)
            builder[0].from_dict({"err": {"code": 1, "message": "test error"}})
            builder[1].from_dict({"err": {"code": 1, "message": "test2 error"}})

        mock_connection.server.setValue.side_effect = mock_method

        with pytest.raises(errors.LabOneCoreError, match="test error"):
            await mock_connection.session.set(value)

    @pytest.mark.asyncio()
    async def test_illegal_input_list_int(self, mock_connection):
        value = AnnotatedValue(value=123, path=["/bar/foobar", "/foo/bar"], timestamp=0)
        with pytest.raises(TypeError):
            await mock_connection.session.set(value)


class TestSetValueWithPathExpression:
    """Integration tests for Session node set values functionality."""

    @pytest.mark.asyncio()
    async def test_server_receives_correct_value(self, mock_connection):
        recorder = ServerRecords()

        def mock_method(params, _):
            param_builder = params.as_builder()
            recorder.params.append(param_builder)

        mock_connection.server.setValue.side_effect = mock_method

        value = AnnotatedValue(value=12, path="/foo/bar")
        result = await mock_connection.session.set_with_expression(value)
        assert len(recorder.params) == 1
        assert recorder.params[0].pathExpression == "/foo/bar"
        assert session_proto_value_to_python(recorder.params[0].value) == 12
        assert result == []

    @pytest.mark.asyncio()
    async def test_server_response_ok_single(self, mock_connection, reflection_server):
        value = AnnotatedValue(value=123, path="/bar/foobar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 1)
            builder[0].ok = value.to_capnp(reflection=reflection_server)

        mock_connection.server.setValue.side_effect = mock_method
        response = await mock_connection.session.set_with_expression(value)
        assert response[0] == value

    @pytest.mark.asyncio()
    async def test_server_response_ok_multiple(
        self,
        mock_connection,
        reflection_server,
    ):
        value0 = AnnotatedValue(value=123, path="/bar/foobar", timestamp=0)
        value1 = AnnotatedValue(value=124, path="/bar/foo", timestamp=0)
        value2 = AnnotatedValue(value=125, path="/bar/bar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 3)
            builder[0].ok = value0.to_capnp(reflection=reflection_server)
            builder[1].ok = value1.to_capnp(reflection=reflection_server)
            builder[2].ok = value2.to_capnp(reflection=reflection_server)

        mock_connection.server.setValue.side_effect = mock_method
        response = await mock_connection.session.set_with_expression(value0)
        assert response[0] == value0
        assert response[1] == value1
        assert response[2] == value2

    @pytest.mark.asyncio()
    async def test_server_response_err_single(self, mock_connection):
        value = AnnotatedValue(value=123, path="/bar/foobar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 1)
            builder[0].from_dict({"err": {"code": 1, "message": "test error"}})

        mock_connection.server.setValue.side_effect = mock_method

        with pytest.raises(errors.LabOneCoreError, match="test error"):
            await mock_connection.session.set_with_expression(value)

    @pytest.mark.asyncio()
    async def test_server_response_err_multiple(self, mock_connection):
        value = AnnotatedValue(value=123, path="/bar/foobar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 2)
            builder[0].from_dict({"err": {"code": 1, "message": "test error"}})
            builder[1].from_dict({"err": {"code": 1, "message": "test2 error"}})

        mock_connection.server.setValue.side_effect = mock_method

        with pytest.raises(errors.LabOneCoreError, match="test error"):
            await mock_connection.session.set_with_expression(value)

    @pytest.mark.asyncio()
    async def test_server_response_err_mix(self, mock_connection, reflection_server):
        value = AnnotatedValue(value=123, path="/foo/bar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 2)
            builder[0].ok = value.to_capnp(reflection=reflection_server)
            builder[1].from_dict({"err": {"code": 2, "message": "test2 error"}})

        mock_connection.server.setValue.side_effect = mock_method
        with pytest.raises(errors.LabOneCoreError, match="test2 error"):
            await mock_connection.session.set_with_expression(value)

    @pytest.mark.asyncio()
    async def test_illegal_input_list_int(self, mock_connection):
        value = AnnotatedValue(value=123, path=["/bar/foobar", "/foo/bar"], timestamp=0)
        with pytest.raises(TypeError):
            await mock_connection.session.set(value)


class TestGetValueWithExpression:
    """Integration tests for Session node get values functionality."""

    @pytest.mark.asyncio()
    async def test_server_receives_correct_value(self, mock_connection):
        recorder = ServerRecords()

        def mock_method(params, _):
            param_builder = params.as_builder()
            recorder.params.append(param_builder)

        mock_connection.server.getValue.side_effect = mock_method
        result = await mock_connection.session.get_with_expression("/foo/*")
        assert len(recorder.params) == 1
        assert recorder.params[0].pathExpression == "/foo/*"
        assert result == []

    @pytest.mark.asyncio()
    async def test_server_response_ok_single(self, mock_connection, reflection_server):
        value = AnnotatedValue(value=123, path="/foo/bar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 1)
            builder[0].ok = value.to_capnp(reflection=reflection_server)

        mock_connection.server.getValue.side_effect = mock_method
        response = await mock_connection.session.get_with_expression("/foo/bar")
        assert response[0] == value

    @pytest.mark.asyncio()
    async def test_server_response_ok_multiple(
        self,
        mock_connection,
        reflection_server,
    ):
        value0 = AnnotatedValue(value=123, path="/foo/bar", timestamp=0)
        value1 = AnnotatedValue(value=123, path="/foo/bar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 2)
            builder[0].ok = value0.to_capnp(reflection=reflection_server)
            builder[1].ok = value1.to_capnp(reflection=reflection_server)

        mock_connection.server.getValue.side_effect = mock_method
        response = await mock_connection.session.get_with_expression("/foo/bar")
        assert response[0] == value0
        assert response[1] == value1

    @pytest.mark.asyncio()
    async def test_server_response_err_single(self, mock_connection):
        def mock_method(_, results):
            builder = results.init("result", 1)
            builder[0].from_dict({"err": {"code": 1, "message": "test error"}})

        mock_connection.server.getValue.side_effect = mock_method
        with pytest.raises(errors.LabOneCoreError, match="test error"):
            await mock_connection.session.get_with_expression("/foo/bar")

    @pytest.mark.asyncio()
    async def test_server_response_err_multiple(self, mock_connection):
        def mock_method(_, results):
            builder = results.init("result", 2)
            builder[0].from_dict({"err": {"code": 1, "message": "test error"}})
            builder[1].from_dict({"err": {"code": 2, "message": "test2 error"}})

        mock_connection.server.getValue.side_effect = mock_method
        with pytest.raises(errors.LabOneCoreError, match="test error"):
            await mock_connection.session.get_with_expression("/foo/bar")

    @pytest.mark.asyncio()
    async def test_server_response_err_mix(self, mock_connection, reflection_server):
        value = AnnotatedValue(value=123, path="/foo/bar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 2)
            builder[0].ok = value.to_capnp(reflection=reflection_server)
            builder[1].from_dict({"err": {"code": 2, "message": "test2 error"}})

        mock_connection.server.getValue.side_effect = mock_method
        with pytest.raises(errors.LabOneCoreError, match="test2 error"):
            await mock_connection.session.get_with_expression("/foo/bar")

    @pytest.mark.asyncio()
    async def test_illegal_input_list_string(self, mock_connection):
        with pytest.raises(TypeError):
            await mock_connection.session.get_with_expression(["/foo/bar"])

    @pytest.mark.asyncio()
    async def test_illegal_input_list_int(self, mock_connection):
        with pytest.raises(TypeError):
            await mock_connection.session.get_with_expression([1, 2, 3])


class TestGetValue:
    """Integration tests for Session node get values functionality."""

    @pytest.mark.asyncio()
    async def test_server_receives_correct_values_single(self, mock_connection):
        recorder = ServerRecords()

        def mock_method(params, _):
            param_builder = params.as_builder()
            recorder.params.append(param_builder)

        mock_connection.server.getValue.side_effect = mock_method
        with pytest.raises(errors.LabOneCoreError):
            await mock_connection.session.get("/foo/bar")
        assert len(recorder.params) == 1
        assert recorder.params[0].pathExpression == "/foo/bar"

    @pytest.mark.asyncio()
    async def test_server_response_ok_single(self, mock_connection, reflection_server):
        value = AnnotatedValue(value=123, path="/foo/bar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 1)
            builder[0].ok = value.to_capnp(reflection=reflection_server)

        mock_connection.server.getValue.side_effect = mock_method
        response = await mock_connection.session.get("/foo/bar")
        assert response == value

    @pytest.mark.asyncio()
    async def test_server_response_err_single(self, mock_connection):
        def mock_method(_, results):
            builder = results.init("result", 1)
            builder[0].from_dict({"err": {"code": 1, "message": "test error"}})

        mock_connection.server.getValue.side_effect = mock_method
        with pytest.raises(errors.LabOneCoreError, match="test error"):
            await mock_connection.session.get("/foo/bar")

    @pytest.mark.asyncio()
    async def test_server_response_err_multiple(self, mock_connection):
        def mock_method(_, results):
            builder = results.init("result", 2)
            builder[0].from_dict({"err": {"code": 1, "message": "test error"}})
            builder[1].from_dict({"err": {"code": 2, "message": "test2 error"}})

        mock_connection.server.getValue.side_effect = mock_method
        with pytest.raises(errors.LabOneCoreError, match="test error"):
            await mock_connection.session.get("/foo/bar")

    @pytest.mark.asyncio()
    async def test_illegal_input_list_string(self, mock_connection):
        with pytest.raises(TypeError):
            await mock_connection.session.get(["/foo/bar"])

    @pytest.mark.asyncio()
    async def test_illegal_input_list_int(self, mock_connection):
        with pytest.raises(TypeError):
            await mock_connection.session.get([1, 2, 3])


class TestSessionSubscribe:
    class SubscriptionServer:
        def __init__(self, error=None):
            self.server_handle = None
            self.path = None
            self.client_id = None
            self.error = error

        def subscribe(self, params, results):
            self.path = params.subscription.path
            self.client_id = params.subscription.subscriberId
            self.server_handle = params.subscription.streamingHandle
            if self.error:
                results.result.from_dict({"err": {"code": 1, "message": self.error}})
            else:
                results.result.from_dict({"ok": {}})

    @pytest.mark.asyncio()
    async def test_subscribe_meta_data(self, mock_connection):
        path = "/dev1234/demods/0/sample"
        subscription_server = self.SubscriptionServer()
        mock_connection.server.subscribe.side_effect = subscription_server.subscribe
        queue = await mock_connection.session.subscribe(path)
        assert subscription_server.path == path
        assert subscription_server.client_id == mock_connection.session._client_id.bytes
        assert queue.qsize() == 0
        assert queue.path == path

    @pytest.mark.asyncio()
    async def test_subscribe_error(self, mock_connection):
        path = "/dev1234/demods/0/sample"
        subscription_server = self.SubscriptionServer(error="test error")
        mock_connection.server.subscribe.side_effect = subscription_server.subscribe

        with pytest.raises(errors.LabOneCoreError, match="test error"):
            await mock_connection.session.subscribe(path)

    @pytest.mark.asyncio()
    async def test_subscribe_invalid_argument_dict(self, mock_connection):
        with pytest.raises(TypeError):
            await mock_connection.session.subscribe({"I": "am", "not": "a", "path": 1})

    @pytest.mark.asyncio()
    async def test_subscribe_invalid_argument_int(self, mock_connection):
        with pytest.raises(TypeError):
            await mock_connection.session.subscribe(2)

    @pytest.mark.parametrize("num_values", range(0, 20, 4))
    @pytest.mark.asyncio()
    async def test_subscribe_send_value_ok(self, mock_connection, num_values):
        path = "/dev1234/demods/0/sample"
        subscription_server = self.SubscriptionServer()
        mock_connection.server.subscribe.side_effect = subscription_server.subscribe
        queue = await mock_connection.session.subscribe(path)

        values = []
        for i in range(num_values):
            value = value_capnp.AnnotatedValue.new_message()
            value.metadata.path = path
            value.value.int64 = i
            values.append(value)
        value = value_capnp.AnnotatedValue.new_message()
        value.metadata.path = "dummy"
        value.value.int64 = 1
        await subscription_server.server_handle.sendValues(values)
        assert queue.qsize() == num_values
        for i in range(num_values):
            assert queue.get_nowait() == AnnotatedValue(
                value=i,
                path=path,
                timestamp=0,
                extra_header=None,
            )

    @pytest.mark.asyncio()
    async def test_subscribe_get_initial(self, mock_connection):
        path = "/dev1234/demods/0/sample"
        subscription_server = self.SubscriptionServer()
        mock_connection.server.subscribe.side_effect = subscription_server.subscribe
        mock_connection.session.get = AsyncMock(
            return_value=AnnotatedValue(value=1, path=path),
        )
        queue = await mock_connection.session.subscribe(path, get_initial_value=True)

        assert queue.qsize() == 1
        assert queue.get_nowait() == AnnotatedValue(value=1, path=path)


class TestSessionWaitForStateChange:
    def create_queue(self, value, path):
        queue = MagicMock(spec=DataQueue)
        value = iter(cycle([value])) if isinstance(value, int) else iter(value)

        async def mock_queue_get():
            await asyncio.sleep(0.01)
            return AnnotatedValue(value=next(value), path=path)

        queue.get.side_effect = mock_queue_get
        return queue

    @pytest.mark.asyncio()
    async def test_wait_for_state_change_already_correct(self, mock_connection):
        path = "/foo/bar"

        mock_connection.session.subscribe = AsyncMock(
            return_value=self.create_queue(1, path),
        )

        await asyncio.wait_for(
            mock_connection.session.wait_for_state_change("/foo/bar", 1),
            0.1,
        )

        mock_connection.session.subscribe.assert_called_once_with(
            path,
            get_initial_value=True,
        )

    @pytest.mark.asyncio()
    async def test_wait_for_state_change_timeout(self, mock_connection):
        path = "/foo/bar"

        mock_connection.session.subscribe = AsyncMock(
            return_value=self.create_queue(999, path),
        )

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                mock_connection.session.wait_for_state_change("/foo/bar", 1),
                0.1,
            )

        mock_connection.session.subscribe.assert_called_once_with(
            path,
            get_initial_value=True,
        )

    @pytest.mark.asyncio()
    async def test_wait_for_state_change_ok(self, mock_connection):
        path = "/foo/bar"

        mock_connection.session.subscribe = AsyncMock(
            return_value=self.create_queue([0, 0, 1], path),
        )

        await asyncio.wait_for(
            mock_connection.session.wait_for_state_change("/foo/bar", 1),
            0.1,
        )

        mock_connection.session.subscribe.assert_called_once_with(
            path,
            get_initial_value=True,
        )


class BrokenSessionBootstrap(session_protocol_capnp.Session.Server):
    """A bootstrap of `labone.core.resource.session_protocol.Session` Server"""

    def __init__(self, mock):
        self._mock = mock


class TestKJErrors:
    @pytest.mark.asyncio()
    async def test_session_function_not_implemented(self, reflection_server):
        mock_mock_connection = MagicMock()
        broken_server = await CapnpServer.create(
            BrokenSessionBootstrap(mock_mock_connection),
        )
        reflection = reflection_server
        client = capnp.TwoPartyClient(broken_server.connection)
        reflection.session = client.bootstrap().cast_as(reflection.Session)
        client = KernelSession(
            reflection_server=reflection,
            kernel_info=ZIKernelInfo(),
            server_info=ServerInfo(host="localhost", port=8004),
        )
        with pytest.raises(errors.UnavailableError):
            await client.list_nodes("test")


@pytest.mark.asyncio()
async def test_set_transaction_no_wrapper():
    session = await spawn_hpk_mock(
        AutomaticSessionFunctionality({"a": {"Type": "Integer"}}),
    )
    target_value = 4656
    async with session.set_transaction() as transaction:
        transaction.append(session.set(AnnotatedValue(path="a", value=target_value)))
    assert (await session.get("a")).value == target_value
    assert await session._supports_transaction() is False


@pytest.mark.asyncio()
async def test_set_transaction_additional_futures():
    session = await spawn_hpk_mock(
        AutomaticSessionFunctionality({"a": {"Type": "Integer"}}),
    )
    target_value = 4656
    async with session.set_transaction() as transaction:
        transaction.append(session.set(AnnotatedValue(path="a", value=target_value)))
        transaction.append(session.get("a"))
        transaction.append(asyncio.sleep(0.001))
    assert (await session.get("a")).value == target_value


@pytest.mark.asyncio()
async def test_set_transaction_wrapper():
    session = await spawn_hpk_mock(
        AutomaticSessionFunctionality(
            {
                "a": {"Type": "Integer"},
                "/ctrl/transaction/state": {"Type": "Integer"},
            },
        ),
    )
    subscription_state = await session.subscribe("/ctrl/transaction/state")
    target_value = 4656

    # Use function so that we can store the acknowledged value which required to
    # compare the timestamps
    result = None

    async def set_value():
        nonlocal result
        result = await session.set(AnnotatedValue(path="a", value=target_value))

    async with session.set_transaction() as transaction:
        transaction.append(set_value())
    assert (await session.get("a")).value == target_value

    assert subscription_state.qsize() == 2
    start = subscription_state.get_nowait()
    assert start.value == 1
    end = subscription_state.get_nowait()
    assert end.value == 0
    assert start.timestamp < result.timestamp < end.timestamp
    assert await session._supports_transaction() is True


@pytest.mark.asyncio()
async def test_set_transaction_multiple_devices():
    session_a = await spawn_hpk_mock(
        AutomaticSessionFunctionality({"a": {"Type": "Integer"}}),
    )
    session_b = await spawn_hpk_mock(
        AutomaticSessionFunctionality(
            {
                "a": {"Type": "Integer"},
                "/ctrl/transaction/state": {"Type": "Integer"},
            },
        ),
    )
    async with AsyncExitStack() as stack:
        transaction_a = await stack.enter_async_context(session_a.set_transaction())
        transaction_b = await stack.enter_async_context(session_b.set_transaction())
        transaction_a.append(session_a.set(AnnotatedValue(path="a", value=1)))
        transaction_b.append(session_b.set(AnnotatedValue(path="a", value=2)))

    assert (await session_a.get("a")).value == 1
    assert (await session_b.get("a")).value == 2


@pytest.mark.asyncio()
async def test_set_transaction_mix_multiple_devices():
    session_a = await spawn_hpk_mock(
        AutomaticSessionFunctionality({"a": {"Type": "Integer"}}),
    )
    session_b = await spawn_hpk_mock(
        AutomaticSessionFunctionality(
            {
                "a": {"Type": "Integer"},
                "/ctrl/transaction/state": {"Type": "Integer"},
            },
        ),
    )
    async with session_b.set_transaction() as transaction:
        transaction.append(session_a.set(AnnotatedValue(path="a", value=1)))
        transaction.append(session_b.set(AnnotatedValue(path="a", value=2)))

    assert (await session_a.get("a")).value == 1
    assert (await session_b.get("a")).value == 2


class DummyServerVersionTest(SessionInterface):
    def __init__(self, version: str):
        super().__init__(None)
        self._version = version

    async def getSessionVersion(self, _context):  # noqa: N802
        return str(self._version)


@pytest.mark.parametrize(
    ("version", "should_fail"),
    [
        (Session.MIN_CAPABILITY_VERSION, False),
        (Session.TESTED_CAPABILITY_VERSION, False),
        ("1.0.0", True),
        ("0.0.0", True),
        (f"{Session.TESTED_CAPABILITY_VERSION.major +1}.0.0", True),
    ],
)
@pytest.mark.asyncio()
async def test_ensure_compatibility_mismatch(version, should_fail):
    mock_server, client_connection = await start_local_server(
        schema=get_schema(),
        server=DummyServerVersionTest(version),
    )
    reflection = await ReflectionServer.create_from_connection(client_connection)
    session = MockSession(
        mock_server,
        reflection.session,  # type: ignore[attr-defined]
        reflection=reflection,
    )
    if should_fail:
        with pytest.raises(errors.UnavailableError):
            await session.ensure_compatibility()
    else:
        await session.ensure_compatibility()
