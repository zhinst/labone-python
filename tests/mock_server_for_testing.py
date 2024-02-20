"""Helper for using mock server for high level testing."""

from __future__ import annotations

import typing as t
from unittest.mock import Mock

from labone.core.session import NodeInfo, Session
from labone.mock.automatic_session_functionality import AutomaticSessionFunctionality
from labone.mock.convert_to_add_nodes import list_nodes_info_to_get_nodes
from labone.mock.entry_point import spawn_hpk_mock
from labone.nodetree.entry_point import construct_nodetree
from labone.nodetree.helper import split_path
from labone.nodetree.new_node import Segment, create_node_tree

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
    functionality = AutomaticSessionFunctionality(nodes_to_info)
    session_mock = await spawn_hpk_mock(functionality)
    return (
        await create_node_tree(session_mock, hide_kernel_prefix=hide_kernel_prefix)
        # await construct_nodetree(
        #     session_mock,
        #     hide_kernel_prefix=hide_kernel_prefix,
        #     custom_parser=custom_parser,
        # )
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
    session_mock.list_nodes_info.return_value = (
        nodes_to_info  # required for construction
    )
    session_mock.get_nodes.return_value = list_nodes_info_to_get_nodes(nodes_to_info)
    return (
        await create_node_tree(session_mock, hide_kernel_prefix=hide_kernel_prefix) #construct_nodetree(session_mock, hide_kernel_prefix=hide_kernel_prefix)
    )
