"""Tests for `labone.core.session.Session` functionality that requires a server."""

import json
import random
from typing import Any
from unittest.mock import MagicMock

import capnp
import pytest
from labone.core import errors
from labone.core.connection_layer import ServerInfo, ZIKernelInfo
from labone.core.resources import session_protocol_capnp  # type: ignore[attr-defined]
from labone.core.session import (
    ListNodesFlags,
    ListNodesInfoFlags,
    Session,
    _send_and_wait_request,
)

from . import utils


class SessionBootstrap(session_protocol_capnp.Session.Server):
    """A bootstrap of `labone.core.resource.session_protocol.Session` Server"""

    def __init__(self, mock):
        self._mock = mock

    async def listNodes(self, _context, **_):  # noqa: N802
        return self._mock.listNodes(_context.params, _context.results)

    async def listNodesJson(self, _context, **_):  # noqa: N802
        return self._mock.listNodesJson(_context.params, _context.results)


@pytest.fixture()
async def session_server() -> tuple[Session, MagicMock]:
    """Fixture for `labone.core.Session` and the server it is connected to.

    Returns:
        Session and a server mock, which is passed into `SessionBootstrap`.
    """
    mock_session_server = MagicMock()
    server = await utils.CapnpServer.create(SessionBootstrap(mock_session_server))
    session = Session(
        connection=server.connection,
        kernel_info=ZIKernelInfo(),
        server_info=ServerInfo(host="localhost", port=8004),
    )
    return session, mock_session_server


class TestSessionListNodes:
    @staticmethod
    def mock_return_value(val: list) -> callable:
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
    @pytest.mark.parametrize("flags", ["foo", 1.2, [3], None])
    async def test_with_flags_type_error(self, session_server, flags):
        session, _ = await session_server
        with pytest.raises(errors.LabOneCoreError):
            await session.list_nodes("path", flags=flags)

    @utils.ensure_event_loop
    @pytest.mark.parametrize("flags", [-2, -100])
    async def test_with_flags_value_error(self, session_server, flags):
        session, _ = await session_server
        with pytest.raises(errors.LabOneCoreError):
            await session.list_nodes("path", flags=flags)


class TestSessionListNodesJson:
    @staticmethod
    def mock_return_value(val: dict) -> callable:
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
    @pytest.mark.parametrize("flags", ["foo", 1.2, [3], None])
    async def test_with_flags_type_error(self, session_server, flags):
        session, _ = await session_server
        with pytest.raises(errors.LabOneCoreError):
            await session.list_nodes_info("path", flags=flags)

    @utils.ensure_event_loop
    @pytest.mark.parametrize("flags", [-2, -100])
    async def test_with_flags_value_error(self, session_server, flags):
        session, _ = await session_server
        with pytest.raises(errors.LabOneCoreError):
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
