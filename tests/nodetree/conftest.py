from __future__ import annotations

import json
import typing as t
from functools import cached_property
from pathlib import Path
from unittest.mock import MagicMock

import pytest

if t.TYPE_CHECKING:
    from labone.core.subscription import DataQueue
    from labone.nodetree.enum import NodeEnum
from labone.nodetree.helper import (
    NormalizedPathSegment,
    join_path,
    paths_to_nested_dict,
)
from labone.nodetree.node import (
    LeafNode,
    MetaNode,
    NodeTreeManager,
    PartialNode,
    ResultNode,
    WildcardNode,
)

if t.TYPE_CHECKING:
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
def session_mock():
    """Mock a Session connection by redefining multiple methods."""
    mock = MagicMock()

    async def mock_get(path):
        return zi_get_responses_prop[path]  # will only work for leaf nodes

    async def mock_get_with_expression(*_, **__):
        return list(zi_get_responses)  # will give a dummy answer, not the correct one!

    async def mock_list_nodes_info(*_, **__):
        return zi_structure.nodes_to_info

    async def mock_list_nodes(path):
        if path == "/zi/*/level":
            return [join_path(("zi", "debug", "level"))]
        raise NotImplementedError

    async def mock_set(annotated_value, *_, **__):
        """will NOT change state for later get requests!"""
        return annotated_value

    # set_with_expression
    async def mock_set_with_expression(ann_value, *_, **__):
        """will NOT change state for later get requests!"""
        if ann_value.path == "/zi/*/level":
            return [ann_value]
        raise NotImplementedError

    mock.get.side_effect = mock_get
    mock.get_with_expression.side_effect = mock_get_with_expression
    mock.list_nodes_info.side_effect = mock_list_nodes_info
    mock.set.side_effect = mock_set
    mock.list_nodes.side_effect = mock_list_nodes
    mock.set_with_expression.side_effect = mock_set_with_expression
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
