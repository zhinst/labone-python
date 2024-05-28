"""Helper for using mock server for high level testing."""

from __future__ import annotations

import typing as t
from unittest.mock import AsyncMock, Mock

from labone.core.session import NodeInfo, Session
from labone.mock import AutomaticLabOneServer
from labone.nodetree.entry_point import construct_nodetree

if t.TYPE_CHECKING:
    from labone.core.value import AnnotatedValue
    from labone.nodetree.node import Node


async def get_mocked_node(
    nodes_to_info: dict[str, NodeInfo],
    *,
    hide_kernel_prefix: bool = False,
    custom_parser: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
) -> Node:
    """Uses custom mock implementation.

    Use when testing calls to the server.
    """
    session_mock = await AutomaticLabOneServer(nodes_to_info).start_pipe()
    return (
        await construct_nodetree(
            session_mock,
            hide_kernel_prefix=hide_kernel_prefix,
            custom_parser=custom_parser,
        )
    ).root


async def get_unittest_mocked_node(
    nodes_to_info: dict[str, NodeInfo],
    *,
    hide_kernel_prefix: bool = False,
) -> Node:
    """Minimal unittest mock.

    Use when no calls to the server are tested.
    This way, the tests do not depend on the `labone`
    mock server.
    """
    session_mock = Mock(spec=Session)
    session_mock.list_nodes_info = AsyncMock(return_value=nodes_to_info)
    session_mock.get_with_expression = AsyncMock()

    return (
        await construct_nodetree(session_mock, hide_kernel_prefix=hide_kernel_prefix)
    ).root
