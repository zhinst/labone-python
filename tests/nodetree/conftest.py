from __future__ import annotations

import asyncio
import fnmatch
import json
import typing as t
from functools import cached_property
from pathlib import Path
from unittest.mock import PropertyMock, create_autospec, patch

import pytest
from labone.core import KernelSession
from labone.core.subscription import DataQueue

if t.TYPE_CHECKING:
    from labone.nodetree.enum import NodeEnum
from labone.nodetree.helper import (
    NormalizedPathSegment,
    paths_to_nested_dict,
)
from labone.nodetree.node import (
    LeafNode,
    MetaNode,
    NodeTreeManager,
    PartialNode,
    ResultNode,
    WildcardNode,
    WildcardOrPartialNode,
)

from src.labone.core.value import AnnotatedValue
from src.labone.nodetree.node import Node
from tests.nodetree.zi_responses import zi_get_responses


class StructureProvider:
    """Helper class to provide diverse useful formats, given a node-to-info mapping."""

    def __init__(self, nodes_to_info):
        self._nodes_to_info = nodes_to_info

    @cached_property
    def nodes_to_info(self):
        return self._nodes_to_info

    @cached_property
    def paths(self):
        return self._nodes_to_info.keys()

    @cached_property
    def structure(self):
        return paths_to_nested_dict(self.paths)


@pytest.fixture()
def data_dir() -> Path:
    return Path(__file__).parent / "resources"


@pytest.fixture()
def zi_structure(data_dir) -> StructureProvider:
    with Path.open(data_dir / "zi_nodes_info.json") as f:
        return StructureProvider(json.load(f))


@pytest.fixture()
def zi_get_responses_prop() -> dict[str, AnnotatedValue]:
    return {ann.path: ann for ann in zi_get_responses}


@pytest.fixture()
def device_id() -> str:
    return "dev1234"


@pytest.fixture()
def device_structure(data_dir) -> StructureProvider:
    with Path.open(data_dir / "device_nodes_info.json") as f:
        return StructureProvider(json.load(f))


@pytest.fixture()
def session_mock(zi_structure, zi_get_responses_prop):
    """Mock a Session connection by redefining multiple methods."""
    device_state = {}
    mock = create_autospec(KernelSession)
    subscription_queues = {}

    async def mock_list_nodes(path):
        if path[-1] != "*":
            path = path + "/*"
        return fnmatch.filter(
            zi_structure.paths,
            path,
        )  # [p for p in zi_structure.paths if fnmatch.fnmatch(p,path)]

    async def mock_get(path):
        return device_state.get(path, zi_get_responses_prop[path])

    async def mock_set(annotated_value, *_, **__):
        device_state[annotated_value.path] = annotated_value
        if annotated_value.path in subscription_queues:
            await subscription_queues[annotated_value.path].put(annotated_value)
        return annotated_value

    async def mock_get_with_expression(path, **__):
        paths = await mock_list_nodes(path)
        return [await mock_get(p) for p in paths]

    async def mock_set_with_expression(ann_value, *_, **__):
        paths = await mock_list_nodes(ann_value.path)
        return [
            await mock_set(AnnotatedValue(path=p, value=ann_value.value)) for p in paths
        ]

    async def mock_list_nodes_info(path="*"):
        return {
            p: zi_structure.nodes_to_info[p]
            for p in zi_structure.paths
            if fnmatch.fnmatch(p, path)
        }

    async def subscribe(path, **__):
        subscription_queues[path] = DataQueue(
            path=path,
            register_function=lambda _: None,
        )
        return subscription_queues[path]

    mock.list_nodes.side_effect = mock_list_nodes
    mock.list_nodes_info.side_effect = mock_list_nodes_info
    mock.get.side_effect = mock_get
    mock.get_with_expression.side_effect = mock_get_with_expression
    mock.set.side_effect = mock_set
    mock.set_with_expression.side_effect = mock_set_with_expression
    mock.subscribe.side_effect = subscribe

    return mock


@pytest.fixture()
async def session_zi(session_mock, zi_structure) -> Node:
    """Fixture to provide a zi node tree with mock session connection."""
    return (
        NodeTreeManager(
            session=session_mock,
            path_to_info=zi_structure.nodes_to_info,
            parser=lambda x: x,
        )
    ).construct_nodetree(
        hide_kernel_prefix=True,
    )


@pytest.fixture()
def sessionless_manager(zi_structure, request) -> NodeTreeManager:
    """Fixture to provide a zi node tree manager."""
    nodes_to_info_marker = request.node.get_closest_marker("nodes_to_info")
    parser_marker = request.node.get_closest_marker("parser_builder")

    def parser(x):
        return x if parser_marker is None else parser_marker.args[0]

    if nodes_to_info_marker is None:
        nodes_to_info = zi_structure.nodes_to_info
    else:
        nodes_to_info = nodes_to_info_marker.args[0]

    return NodeTreeManager(
        session=None,
        path_to_info=nodes_to_info,
        parser=parser,
    )


@pytest.fixture()
def zi(request, zi_structure) -> Node:
    """Fixture to provide a zi node tree."""
    nodes_to_info_marker = request.node.get_closest_marker("nodes_to_info")
    parser_marker = request.node.get_closest_marker("parser")

    def parser(x):
        return x if parser_marker is None else parser_marker.args[0]

    if nodes_to_info_marker is None:
        nodes_to_info = zi_structure.nodes_to_info
    else:
        nodes_to_info = nodes_to_info_marker.args[0]

    return NodeTreeManager(
        session=None,
        path_to_info=nodes_to_info,
        parser=parser,
    ).construct_nodetree(hide_kernel_prefix=True)


@pytest.fixture()
def result_node(sessionless_manager, zi_structure, zi_get_responses_prop) -> ResultNode:
    """Provide authentic result node"""
    return ResultNode(
        tree_manager=sessionless_manager,
        path_segments=(),
        subtree_paths=zi_structure.structure,
        value_structure=zi_get_responses_prop,
        timestamp=0,
    ).zi


class MockMetaNode(MetaNode):
    """Get simple MetaNode like object by path"""

    def __init__(self, path_segments):
        super().__init__(
            path_segments=path_segments,
            tree_manager=None,
            subtree_paths=None,
            path_aliases=None,
        )

    def __getattr__(self, item):
        return self

    def __getitem__(self, item):
        return self


class MockResultNode(ResultNode):
    """Get simple ResultNode like object by path"""

    def __init__(self, path_segments):
        super().__init__(
            path_segments=path_segments,
            tree_manager=None,
            subtree_paths=None,
            path_aliases=None,
            value_structure=None,
            timestamp=None,
        )


class MockNode(Node):
    """Get simple Node like object by path"""

    def __init__(self, path_segments):
        super().__init__(
            path_segments=path_segments,
            tree_manager=None,
            subtree_paths=None,
            path_aliases=None,
        )

    def _get(self, *_, **__):
        return

    def _set(self, *_, **__):
        return

    def subscribe(self) -> DataQueue:
        return

    def try_generate_subnode(
        self,
        next_path_segment: NormalizedPathSegment,  # noqa: ARG002
    ) -> Node:
        return

    def wait_for_state_change(
        self,
        value: int | NodeEnum,  # noqa: ARG002
        *,
        invert: bool = False,  # noqa: ARG002
        timeout: float = 2,  # noqa: ARG002
    ) -> None:
        return


class MockWildcardOrPartialNode(WildcardOrPartialNode):
    def __init__(self, path_segments):
        super().__init__(
            tree_manager=None,
            path_segments=path_segments,
            subtree_paths=None,
            path_aliases=None,
        )

    def _package_get_response(self, *_, **__):
        return

    def wait_for_state_change(
        self,
        value: int | NodeEnum,  # noqa: ARG002
        *,
        invert: bool = False,  # noqa: ARG002
        timeout: float = 2,  # noqa: ARG002
    ) -> None:
        return

    def try_generate_subnode(
        self,
        next_path_segment: NormalizedPathSegment,  # noqa: ARG002
    ) -> Node:
        return


class MockPartialNode(PartialNode):
    def __init__(self, path_segments):
        super().__init__(
            tree_manager=None,
            path_segments=path_segments,
            subtree_paths=None,
            path_aliases=None,
        )


class MockLeafNode(LeafNode):
    def __init__(self, path_segments):
        super().__init__(
            tree_manager=None,
            path_segments=path_segments,
            subtree_paths=None,
            path_aliases=None,
        )


class MockWildcardNode(WildcardNode):
    def __init__(self, path_segments):
        super().__init__(
            tree_manager=None,
            path_segments=path_segments,
            subtree_paths=None,
            path_aliases=None,
        )


def _get_future(value):
    future = asyncio.Future()
    future.set_result(value)
    return future


@pytest.fixture()
def mock_path():
    with patch(
        "labone.nodetree.node.MetaNode.path",
        new_callable=PropertyMock,
        return_value="path",
    ) as path_patch:
        yield path_patch
