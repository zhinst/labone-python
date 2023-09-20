"""Tests for `labone.core.session.Session` functionality that requires a server."""

from __future__ import annotations

import json
import random
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable
from unittest.mock import MagicMock

import capnp
import pytest
from labone.core import errors
from labone.core.connection_layer import ServerInfo, ZIKernelInfo
from labone.core.helper import request_field_type_description
from labone.core.resources import session_protocol_capnp  # type: ignore[attr-defined]
from labone.core.session import (
    KernelSession,
    ListNodesFlags,
    ListNodesInfoFlags,
    _send_and_wait_request,
)
from labone.core.value import AnnotatedValue

from . import utils
from .resources import testfile_capnp  # type: ignore[attr-defined]


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


@pytest.fixture()
async def session_server() -> tuple[KernelSession, MagicMock]:
    """Fixture for `labone.core.Session` and the server it is connected to.

    Returns:
        Session and a server mock, which is passed into `SessionBootstrap`.
    """
    mock_session_server = MagicMock()
    server = await utils.CapnpServer.create(SessionBootstrap(mock_session_server))
    session = KernelSession(
        connection=server.connection,
        kernel_info=ZIKernelInfo(),
        server_info=ServerInfo(host="localhost", port=8004),
    )
    return session, mock_session_server


class TestSessionListNodes:
    @staticmethod
    def mock_return_value(val: list) -> Callable:
        def mock_method(_, results):
            results.paths = val

        return mock_method

    @utils.ensure_event_loop
    @pytest.mark.parametrize(
        ("from_server", "from_api"),
        [
            (["foo", "bar"], ["foo", "bar"]),
            ([], []),
            ([""], [""]),
        ],
    )
    async def test_return_value(self, session_server, from_server, from_api):
        session, server = await session_server
        server.listNodes.side_effect = self.mock_return_value(from_server)
        r = await session.list_nodes("path")
        assert r == from_api

    @utils.ensure_event_loop
    @pytest.mark.parametrize("path", [1, 1.1, ["a"], {"a": "b"}])
    async def test_invalid_path_type(self, session_server, path):
        session, _ = await session_server
        with pytest.raises(TypeError):
            await session.list_nodes(path)

    @utils.ensure_event_loop
    @pytest.mark.parametrize("flags", list(ListNodesFlags))
    async def test_with_flags_enum(self, session_server, flags):
        session, server = await session_server
        server.listNodes.side_effect = self.mock_return_value([])
        r = await session.list_nodes("path", flags=flags)
        assert r == []

    @utils.ensure_event_loop
    @pytest.mark.parametrize(
        "flags",
        [random.randint(0, 10000) for _ in range(5)],  # noqa: S311
    )
    async def test_with_flags_int(self, session_server, flags):
        session, server = await session_server
        server.listNodes.side_effect = self.mock_return_value([])
        r = await session.list_nodes("path", flags=flags)
        assert r == []

    @utils.ensure_event_loop
    @pytest.mark.parametrize("flags", ["foo", [3], None])
    async def test_with_flags_type_error(self, session_server, flags):
        session, _ = await session_server
        with pytest.raises(TypeError):
            await session.list_nodes("path", flags=flags)

    @utils.ensure_event_loop
    @pytest.mark.parametrize("flags", [-2, -100])
    async def test_with_flags_value_error(self, session_server, flags):
        session, _ = await session_server
        with pytest.raises(ValueError):
            await session.list_nodes("path", flags=flags)


class TestSessionListNodesJson:
    @staticmethod
    def mock_return_value(val: dict) -> Callable:
        def mock_method(_, results):
            results.nodeProps = json.dumps(val)

        return mock_method

    @utils.ensure_event_loop
    @pytest.mark.parametrize(
        ("from_server", "from_api"),
        [
            ({"foo": "bar", "bar": "foo"}, {"foo": "bar", "bar": "foo"}),
            ({}, {}),
        ],
    )
    async def test_return_value(self, session_server, from_server, from_api):
        session, server = await session_server
        server.listNodesJson.side_effect = self.mock_return_value(from_server)
        r = await session.list_nodes_info("path")
        assert r == from_api

    @utils.ensure_event_loop
    @pytest.mark.parametrize("path", [1, 1.1, ["a"], {"a": "b"}])
    async def test_invalid_path_type(self, session_server, path):
        session, _ = await session_server
        with pytest.raises(TypeError):
            await session.list_nodes_info(path)

    @utils.ensure_event_loop
    @pytest.mark.parametrize("flags", list(ListNodesInfoFlags))
    async def test_with_flags_enum(self, session_server, flags):
        session, server = await session_server
        server.listNodesJson.side_effect = self.mock_return_value({})
        r = await session.list_nodes_info("path", flags=flags)
        assert r == {}

    @utils.ensure_event_loop
    @pytest.mark.parametrize(
        "flags",
        [random.randint(0, 10000) for _ in range(5)],  # noqa: S311
    )
    async def test_with_flags_int(self, session_server, flags):
        session, server = await session_server
        server.listNodesJson.side_effect = self.mock_return_value({})
        r = await session.list_nodes_info("path", flags=flags)
        assert r == {}

    @utils.ensure_event_loop
    @pytest.mark.parametrize("flags", ["foo", [3], None])
    async def test_with_flags_type_error(self, session_server, flags):
        session, _ = await session_server
        with pytest.raises(TypeError):
            await session.list_nodes_info("path", flags=flags)

    @utils.ensure_event_loop
    @pytest.mark.parametrize("flags", [-2, -100])
    async def test_with_flags_value_error(self, session_server, flags):
        session, _ = await session_server
        with pytest.raises(ValueError):
            await session.list_nodes_info("path", flags=flags)


class MockRemotePromise:
    """Mock of `capnp.lib.capnp._RemotePromise`"""

    def __init__(self, a_wait_response: Any = None, a_wait_raise_for: Any = None):
        self._a_wait_response = a_wait_response
        self._a_wait_raise_for = a_wait_raise_for

    async def a_wait(self):
        if self._a_wait_raise_for:
            raise self._a_wait_raise_for
        return self._a_wait_response


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
    @utils.ensure_event_loop
    async def test_success(self):
        promise = MockRemotePromise(a_wait_response="foobar")
        response = await _send_and_wait_request(MockRequest(promise))
        assert response == "foobar"

    @utils.ensure_event_loop
    async def test_send_kj_error(self):
        with pytest.raises(errors.LabOneConnectionError, match="error"):
            await _send_and_wait_request(
                MockRequest(send_raise_for=capnp.lib.capnp.KjException("error")),
            )

    @utils.ensure_event_loop
    @pytest.mark.parametrize("error", [RuntimeError, AttributeError, ValueError])
    async def test_send_misc_error(self, error):
        with pytest.raises(errors.LabOneConnectionError, match="error"):
            await _send_and_wait_request(
                MockRequest(send_raise_for=error("error")),
            )

    @utils.ensure_event_loop
    async def test_suppress_unwanted_traceback(self):
        # Flaky test..
        try:
            await _send_and_wait_request(
                MockRequest(send_raise_for=capnp.lib.capnp.KjException("error")),
            )
        except errors.LabOneConnectionError:
            assert "KjException" not in traceback.format_exc()

        promise = MockRemotePromise(
            a_wait_raise_for=RuntimeError("error"),
        )
        try:
            await _send_and_wait_request(MockRequest(promise))
        except errors.LabOneConnectionError:
            assert "RuntimeError" not in traceback.format_exc()

    @utils.ensure_event_loop
    async def test_a_wait_kj_error(self):
        promise = MockRemotePromise(
            a_wait_raise_for=capnp.lib.capnp.KjException("error"),
        )
        with pytest.raises(errors.LabOneConnectionError, match="error"):
            await _send_and_wait_request(MockRequest(promise))

    @utils.ensure_event_loop
    @pytest.mark.parametrize("error", [RuntimeError, AttributeError, ValueError])
    async def test_a_wait_misc_error(self, error):
        promise = MockRemotePromise(a_wait_raise_for=error("error"))
        with pytest.raises(errors.LabOneConnectionError, match="error"):
            await _send_and_wait_request(MockRequest(promise))


@utils.ensure_event_loop
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

    @pytest.fixture()
    async def session_recorder(
        self,
        session_server,
    ) -> tuple[KernelSession, ServerRecords]:
        session, server = await session_server
        recorder = ServerRecords()

        def mock_method(params, _):
            param_builder = params.as_builder()
            recorder.params.append(param_builder)

        server.setValue.side_effect = mock_method
        return session, recorder

    @utils.ensure_event_loop
    async def test_server_receives_correct_value(self, session_recorder):
        session, recorder = await session_recorder
        value = AnnotatedValue(value=12, path="/foo/bar")
        with pytest.raises(IndexError):
            await session.set(value)
        assert len(recorder.params) == 1
        assert recorder.params[0].pathExpression == "/foo/bar"
        assert session_proto_value_to_python(recorder.params[0].value) == 12

    @utils.ensure_event_loop
    async def test_server_response_ok(self, session_server):
        session, server = await session_server
        value = AnnotatedValue(value=123, path="/bar/foobar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 1)
            builder[0].ok = value.to_capnp()

        server.setValue.side_effect = mock_method
        response = await session.set(value)
        assert response == value

    @utils.ensure_event_loop
    async def test_server_response_err_single(self, session_server):
        session, server = await session_server
        value = AnnotatedValue(value=123, path="/bar/foobar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 1)
            builder[0].from_dict({"err": {"code": 1, "message": "test error"}})

        server.setValue.side_effect = mock_method

        with pytest.raises(errors.LabOneCoreError, match="test error"):
            await session.set(value)

    @utils.ensure_event_loop
    async def test_server_response_err_multiple(self, session_server):
        session, server = await session_server
        value = AnnotatedValue(value=123, path="/bar/foobar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 2)
            builder[0].from_dict({"err": {"code": 1, "message": "test error"}})
            builder[1].from_dict({"err": {"code": 1, "message": "test2 error"}})

        server.setValue.side_effect = mock_method

        with pytest.raises(errors.LabOneCoreError, match="test error"):
            await session.set(value)

    @utils.ensure_event_loop
    async def test_illegal_input_list_int(self, session_server):
        session, _ = await session_server
        value = AnnotatedValue(value=123, path=["/bar/foobar", "/foo/bar"], timestamp=0)
        with pytest.raises(TypeError):
            await session.set(value)


class TestSetValueWithPathExpression:
    """Integration tests for Session node set values functionality."""

    @pytest.fixture()
    async def session_recorder(
        self,
        session_server,
    ) -> tuple[KernelSession, ServerRecords]:
        session, server = await session_server
        recorder = ServerRecords()

        def mock_method(params, _):
            param_builder = params.as_builder()
            recorder.params.append(param_builder)

        server.setValue.side_effect = mock_method
        return session, recorder

    @utils.ensure_event_loop
    async def test_server_receives_correct_value(self, session_recorder):
        session, recorder = await session_recorder
        value = AnnotatedValue(value=12, path="/foo/bar")
        result = await session.set_with_expression(value)
        assert len(recorder.params) == 1
        assert recorder.params[0].pathExpression == "/foo/bar"
        assert session_proto_value_to_python(recorder.params[0].value) == 12
        assert result == []

    @utils.ensure_event_loop
    async def test_server_response_ok_single(self, session_server):
        session, server = await session_server
        value = AnnotatedValue(value=123, path="/bar/foobar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 1)
            builder[0].ok = value.to_capnp()

        server.setValue.side_effect = mock_method
        response = await session.set_with_expression(value)
        assert response[0] == value

    @utils.ensure_event_loop
    async def test_server_response_ok_multiple(self, session_server):
        session, server = await session_server
        value0 = AnnotatedValue(value=123, path="/bar/foobar", timestamp=0)
        value1 = AnnotatedValue(value=124, path="/bar/foo", timestamp=0)
        value2 = AnnotatedValue(value=125, path="/bar/bar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 3)
            builder[0].ok = value0.to_capnp()
            builder[1].ok = value1.to_capnp()
            builder[2].ok = value2.to_capnp()

        server.setValue.side_effect = mock_method
        response = await session.set_with_expression(value0)
        assert response[0] == value0
        assert response[1] == value1
        assert response[2] == value2

    @utils.ensure_event_loop
    async def test_server_response_err_single(self, session_server):
        session, server = await session_server
        value = AnnotatedValue(value=123, path="/bar/foobar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 1)
            builder[0].from_dict({"err": {"code": 1, "message": "test error"}})

        server.setValue.side_effect = mock_method

        with pytest.raises(errors.LabOneCoreError, match="test error"):
            await session.set_with_expression(value)

    @utils.ensure_event_loop
    async def test_server_response_err_multiple(self, session_server):
        session, server = await session_server
        value = AnnotatedValue(value=123, path="/bar/foobar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 2)
            builder[0].from_dict({"err": {"code": 1, "message": "test error"}})
            builder[1].from_dict({"err": {"code": 1, "message": "test2 error"}})

        server.setValue.side_effect = mock_method

        with pytest.raises(errors.LabOneCoreError, match="test error"):
            await session.set_with_expression(value)

    @utils.ensure_event_loop
    async def test_server_response_err_mix(self, session_server):
        session, server = await session_server
        value = AnnotatedValue(value=123, path="/foo/bar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 2)
            builder[0].ok = value.to_capnp()
            builder[1].from_dict({"err": {"code": 2, "message": "test2 error"}})

        server.setValue.side_effect = mock_method
        with pytest.raises(errors.LabOneCoreError, match="test2 error"):
            await session.set_with_expression(value)

    @utils.ensure_event_loop
    async def test_illegal_input_list_int(self, session_server):
        session, _ = await session_server
        value = AnnotatedValue(value=123, path=["/bar/foobar", "/foo/bar"], timestamp=0)
        with pytest.raises(TypeError):
            await session.set(value)


class TestGetValueWithExpression:
    """Integration tests for Session node get values functionality."""

    @pytest.fixture()
    async def session_recorder(
        self,
        session_server,
    ) -> tuple[KernelSession, ServerRecords]:
        session, server = await session_server
        recorder = ServerRecords()

        def mock_method(params, _):
            param_builder = params.as_builder()
            recorder.params.append(param_builder)

        server.getValue.side_effect = mock_method
        return session, recorder

    @utils.ensure_event_loop
    async def test_server_receives_correct_value(self, session_recorder):
        session, recorder = await session_recorder
        result = await session.get_with_expression("/foo/*")
        assert len(recorder.params) == 1
        assert recorder.params[0].pathExpression == "/foo/*"
        assert result == []

    @utils.ensure_event_loop
    async def test_server_response_ok_single(self, session_server):
        session, server = await session_server
        value = AnnotatedValue(value=123, path="/foo/bar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 1)
            builder[0].ok = value.to_capnp()

        server.getValue.side_effect = mock_method
        response = await session.get_with_expression("/foo/bar")
        assert response[0] == value

    @utils.ensure_event_loop
    async def test_server_response_ok_multiple(self, session_server):
        session, server = await session_server
        value0 = AnnotatedValue(value=123, path="/foo/bar", timestamp=0)
        value1 = AnnotatedValue(value=123, path="/foo/bar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 2)
            builder[0].ok = value0.to_capnp()
            builder[1].ok = value1.to_capnp()

        server.getValue.side_effect = mock_method
        response = await session.get_with_expression("/foo/bar")
        assert response[0] == value0
        assert response[1] == value1

    @utils.ensure_event_loop
    async def test_server_response_err_single(self, session_server):
        session, server = await session_server

        def mock_method(_, results):
            builder = results.init("result", 1)
            builder[0].from_dict({"err": {"code": 1, "message": "test error"}})

        server.getValue.side_effect = mock_method
        with pytest.raises(errors.LabOneCoreError, match="test error"):
            await session.get_with_expression("/foo/bar")

    @utils.ensure_event_loop
    async def test_server_response_err_multiple(self, session_server):
        session, server = await session_server

        def mock_method(_, results):
            builder = results.init("result", 2)
            builder[0].from_dict({"err": {"code": 1, "message": "test error"}})
            builder[1].from_dict({"err": {"code": 2, "message": "test2 error"}})

        server.getValue.side_effect = mock_method
        with pytest.raises(errors.LabOneCoreError, match="test error"):
            await session.get_with_expression("/foo/bar")

    @utils.ensure_event_loop
    async def test_server_response_err_mix(self, session_server):
        session, server = await session_server
        value = AnnotatedValue(value=123, path="/foo/bar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 2)
            builder[0].ok = value.to_capnp()
            builder[1].from_dict({"err": {"code": 2, "message": "test2 error"}})

        server.getValue.side_effect = mock_method
        with pytest.raises(errors.LabOneCoreError, match="test2 error"):
            await session.get_with_expression("/foo/bar")

    @utils.ensure_event_loop
    async def test_illegal_input_list_string(self, session_server):
        session, _ = await session_server
        with pytest.raises(TypeError):
            await session.get_with_expression(["/foo/bar"])

    @utils.ensure_event_loop
    async def test_illegal_input_list_int(self, session_server):
        session, _ = await session_server
        with pytest.raises(TypeError):
            await session.get_with_expression([1, 2, 3])


class TestGetValue:
    """Integration tests for Session node get values functionality."""

    @pytest.fixture()
    async def session_recorder(
        self,
        session_server,
    ) -> tuple[KernelSession, ServerRecords]:
        session, server = await session_server
        recorder = ServerRecords()

        def mock_method(params, _):
            param_builder = params.as_builder()
            recorder.params.append(param_builder)

        server.getValue.side_effect = mock_method
        return session, recorder

    @utils.ensure_event_loop
    async def test_server_receives_correct_values_single(self, session_recorder):
        session, recorder = await session_recorder
        with pytest.raises(IndexError):
            await session.get("/foo/bar")
        assert len(recorder.params) == 1
        assert recorder.params[0].pathExpression == "/foo/bar"

    @utils.ensure_event_loop
    async def test_server_response_ok_single(self, session_server):
        session, server = await session_server
        value = AnnotatedValue(value=123, path="/foo/bar", timestamp=0)

        def mock_method(_, results):
            builder = results.init("result", 1)
            builder[0].ok = value.to_capnp()

        server.getValue.side_effect = mock_method
        response = await session.get("/foo/bar")
        assert response == value

    @utils.ensure_event_loop
    async def test_server_response_err_single(self, session_server):
        session, server = await session_server

        def mock_method(_, results):
            builder = results.init("result", 1)
            builder[0].from_dict({"err": {"code": 1, "message": "test error"}})

        server.getValue.side_effect = mock_method
        with pytest.raises(errors.LabOneCoreError, match="test error"):
            await session.get("/foo/bar")

    @utils.ensure_event_loop
    async def test_server_response_err_multiple(self, session_server):
        session, server = await session_server

        def mock_method(_, results):
            builder = results.init("result", 2)
            builder[0].from_dict({"err": {"code": 1, "message": "test error"}})
            builder[1].from_dict({"err": {"code": 2, "message": "test2 error"}})

        server.getValue.side_effect = mock_method
        with pytest.raises(errors.LabOneCoreError, match="test error"):
            await session.get("/foo/bar")

    @utils.ensure_event_loop
    async def test_illegal_input_list_string(self, session_server):
        session, _ = await session_server
        with pytest.raises(TypeError):
            await session.get(["/foo/bar"])

    @utils.ensure_event_loop
    async def test_illegal_input_list_int(self, session_server):
        session, _ = await session_server
        with pytest.raises(TypeError):
            await session.get([1, 2, 3])


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

    @utils.ensure_event_loop
    async def test_subscribe_meta_data(self, session_server):
        session, server = await session_server
        path = "/dev1234/demods/0/sample"
        subscription_server = self.SubscriptionServer()
        server.subscribe.side_effect = subscription_server.subscribe
        queue = await session.subscribe(path)
        assert subscription_server.path == path
        assert subscription_server.client_id == session._client_id.bytes
        assert queue.qsize() == 0
        assert queue.path == path

    @utils.ensure_event_loop
    async def test_subscribe_error(self, session_server):
        session, server = await session_server
        path = "/dev1234/demods/0/sample"
        subscription_server = self.SubscriptionServer(error="test error")
        server.subscribe.side_effect = subscription_server.subscribe

        with pytest.raises(errors.LabOneCoreError, match="test error"):
            await session.subscribe(path)

    @utils.ensure_event_loop
    async def test_subscribe_invalid_argument_dict(self, session_server):
        session, _ = await session_server
        with pytest.raises(TypeError):
            await session.subscribe({"I": "am", "not": "a", "path": 1})

    @utils.ensure_event_loop
    async def test_subscribe_invalid_argument_int(self, session_server):
        session, _ = await session_server
        with pytest.raises(TypeError):
            await session.subscribe(2)

    @pytest.mark.parametrize("num_values", range(0, 20, 4))
    @utils.ensure_event_loop
    async def test_subscribe_send_value_ok(self, session_server, num_values):
        session, server = await session_server
        path = "/dev1234/demods/0/sample"
        subscription_server = self.SubscriptionServer()
        server.subscribe.side_effect = subscription_server.subscribe
        queue = await session.subscribe(path)

        values = []
        for i in range(num_values):
            value = session_protocol_capnp.AnnotatedValue.new_message()
            value.metadata.path = path
            value.value.int64 = i
            values.append(value)
        value = session_protocol_capnp.AnnotatedValue.new_message()
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


class BrokenSessionBootstrap(session_protocol_capnp.Session.Server):
    """A bootstrap of `labone.core.resource.session_protocol.Session` Server"""

    def __init__(self, mock):
        self._mock = mock


class TestKJErrors:
    @utils.ensure_event_loop
    async def test_session_function_not_implemented(self):
        mock_session_server = MagicMock()
        broker_server = await utils.CapnpServer.create(
            BrokenSessionBootstrap(mock_session_server),
        )
        client = KernelSession(
            connection=broker_server.connection,
            kernel_info=ZIKernelInfo(),
            server_info=ServerInfo(host="localhost", port=8004),
        )
        with pytest.raises(errors.LabOneVersionMismatchError):
            await client.list_nodes("test")
