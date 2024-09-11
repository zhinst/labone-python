"""Testing that nodetree calles the correct methods of the session.

The calls are verified via mocking of the lower level interface.
"""

from __future__ import annotations

from unittest.mock import ANY, AsyncMock, Mock

import pytest

from labone.core.session import Session
from labone.core.value import AnnotatedValue
from labone.nodetree.entry_point import construct_nodetree


@pytest.mark.asyncio
async def test_get_translates_to_session():
    value = AnnotatedValue(path="/a/b/c/d", value=42, timestamp=4)

    session_mock = Mock(spec=Session)
    session_mock.list_nodes_info = AsyncMock(return_value={"/a/b/c/d": {}})
    session_mock.get = AsyncMock(return_value=value)
    node = (await construct_nodetree(session_mock, hide_kernel_prefix=False)).root

    assert value == await node.a.b.c.d()
    session_mock.get.assert_called_once_with("/a/b/c/d")


@pytest.mark.asyncio
async def test_set_translates_to_session():
    value = AnnotatedValue(path="/a/b/c/d", value=42, timestamp=4)

    session_mock = Mock(spec=Session)
    session_mock.list_nodes_info = AsyncMock(return_value={"/a/b/c/d": {}})
    session_mock.set = AsyncMock(return_value=value)
    node = (await construct_nodetree(session_mock, hide_kernel_prefix=False)).root

    assert await node.a.b.c.d(42) == value
    session_mock.set.assert_called_once_with(
        AnnotatedValue(path="/a/b/c/d", value=42, timestamp=ANY),
    )


@pytest.mark.asyncio
async def test_partial_get_translates_to_session():
    value = (AnnotatedValue(path="/a/b/c/d", value=42, timestamp=4),)

    session_mock = Mock(spec=Session)
    session_mock.list_nodes_info = AsyncMock(return_value={"/a/b/c/d": {}})
    session_mock.get_with_expression = AsyncMock(return_value=value)
    node = (await construct_nodetree(session_mock, hide_kernel_prefix=False)).root

    await node.a.b.c()
    session_mock.get_with_expression.assert_called_once_with("/a/b/c")


@pytest.mark.asyncio
async def test_partial_set_translates_to_session():
    value = AnnotatedValue(path="/a/b/c/d", value=42, timestamp=4)

    session_mock = Mock(spec=Session)
    session_mock.list_nodes_info = AsyncMock(return_value={"/a/b/c/d": {}})
    session_mock.set_with_expression = AsyncMock(return_value=[value])
    node = (await construct_nodetree(session_mock, hide_kernel_prefix=False)).root

    await node.a.b.c(32)
    session_mock.set_with_expression.assert_called_once_with(
        AnnotatedValue(path="/a/b/c", value=32),
    )


@pytest.mark.asyncio
async def test_wildcard_get_translates_to_session():
    value = (AnnotatedValue(path="/a/b/c/d", value=42, timestamp=4),)

    session_mock = Mock(spec=Session)
    session_mock.list_nodes_info = AsyncMock(return_value={"/a/b/c/d": {}})
    session_mock.get_with_expression = AsyncMock(return_value=value)
    node = (await construct_nodetree(session_mock, hide_kernel_prefix=False)).root

    await node.a["*"].c.d()
    session_mock.get_with_expression.assert_called_once_with("/a/*/c/d")


@pytest.mark.asyncio
async def test_wildcard_set_translates_to_session():
    session_mock = Mock(spec=Session)
    session_mock.list_nodes_info = AsyncMock(return_value={"/a/b/c/d": {}})
    session_mock.set_with_expression = AsyncMock(
        return_value=[
            AnnotatedValue(path="/a/b/c/d", value=42, timestamp=4),
        ],
    )
    node = (await construct_nodetree(session_mock, hide_kernel_prefix=False)).root

    await node.a["*"].c.d(35)
    session_mock.set_with_expression.assert_called_once_with(
        AnnotatedValue(path="/a/*/c/d", value=35),
    )


@pytest.mark.asyncio
async def test_partial_subscribe_translates_to_session():
    session_mock = Mock(spec=Session)
    session_mock.list_nodes_info = AsyncMock(return_value={"/a/b/c/d": {}})
    session_mock.subscribe = AsyncMock(return_value=Mock())
    node = (await construct_nodetree(session_mock, hide_kernel_prefix=False)).root

    await node.a.b.c.d.subscribe()
    session_mock.subscribe.assert_called_once_with(
        "/a/b/c/d",
        parser_callback=ANY,
        queue_type=ANY,
        get_initial_value=ANY,
    )


@pytest.mark.asyncio
async def test_wait_for_state_change_translates_to_session():
    session_mock = Mock(spec=Session)
    session_mock.list_nodes_info = AsyncMock(return_value={"/a/b/c/d": {}})
    node = (await construct_nodetree(session_mock, hide_kernel_prefix=False)).root

    await node.a.b.c.d.wait_for_state_change(value=5, invert=True)

    session_mock.wait_for_state_change.assert_called_once_with(
        "/a/b/c/d",
        5,
        invert=True,
    )


@pytest.mark.asyncio
async def test_wait_for_state_change_wildcard_translates_to_session():
    session_mock = Mock(spec=Session)
    session_mock.list_nodes_info = AsyncMock(return_value={"/a/b": {}, "/a/c": {}})
    session_mock.list_nodes = AsyncMock(return_value=["/a/b", "/a/c"])
    node = (await construct_nodetree(session_mock, hide_kernel_prefix=False)).root

    await node["*"].wait_for_state_change(value=5, invert=True)

    session_mock.wait_for_state_change.assert_any_call("/a/b", 5, invert=True)
    session_mock.wait_for_state_change.assert_any_call("/a/c", 5, invert=True)
