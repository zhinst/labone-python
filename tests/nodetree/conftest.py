import json
from functools import cached_property
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from labone.core import AnnotatedValue
from labone.core.subscription import DataQueue
from labone.nodetree.helper import join_path, paths_to_nested_dict
from labone.nodetree.node import NodeTreeManager, ResultNode

from tests.nodetree.zi_responses import zi_get_responses


class StructureProvider:
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


with Path.open(Path(__file__).parent / "resources" / "zi_nodes_info.json") as f:
    zi_structure = StructureProvider(json.load(f))
zi_get_responses_prop = {ann.path: ann for ann in zi_get_responses}

device_id = "dev12084"
with Path.open(Path(__file__).parent / "resources" / "device_nodes_info.json") as f:
    device_structure = StructureProvider(json.load(f))


def cache(func):
    """Decorator to cache the result of a function call.

    As 3.8 does not have functools.cache yet.
    """
    cache_dict = {}

    def wrapper(*args, **kwargs):
        key = (args, frozenset(kwargs.items()))
        if key not in cache_dict:
            cache_dict[key] = func(*args, **kwargs)
        return cache_dict[key]

    return wrapper


@pytest.fixture
def session_mock():
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

    @cache
    def path_to_unique_queue(path):
        return DataQueue(path=path, register_function=lambda *_, **__: None)

    async def mock_subscribe(path, *_, **__):
        return path_to_unique_queue(path)

    mock.get.side_effect = mock_get
    mock.get_with_expression.side_effect = mock_get_with_expression
    mock.list_nodes_info.side_effect = mock_list_nodes_info
    mock.set.side_effect = mock_set
    mock.subscribe.side_effect = mock_subscribe
    mock.list_nodes.side_effect = mock_list_nodes
    mock.set_with_expression.side_effect = mock_set_with_expression
    return mock


@pytest.fixture
async def session_zi(session_mock):
    return (
        NodeTreeManager(
            session=session_mock,
            path_to_info=zi_structure.nodes_to_info,
            parser=lambda x: x,
        )
    ).construct_nodetree(
        hide_kernel_prefix=True,
    )


def sessionless_manager_logic(
    *,
    nodes_to_info=None,
    parser=None,
):
    if parser is None:

        def parser(x: AnnotatedValue) -> AnnotatedValue:
            return x

    if nodes_to_info is None:
        nodes_to_info = zi_structure.nodes_to_info

    return NodeTreeManager(
        session=None,
        path_to_info=nodes_to_info,
        parser=parser,
    )


@pytest.fixture(name="sessionless_manager")
def sessionless_manager_fixture(
    request# nodes_to_info=None, parser_builder=None,
):
    nodes_to_info_marker = request.node.get_closest_marker("nodes_to_info")
    parser_marker = request.node.get_closest_marker("parser_builder")
    if parser_marker is None:
        parser = lambda x:x
    else:
        parser = parser_marker.args[0]

    if nodes_to_info_marker is None:
        nodes_to_info = zi_structure.nodes_to_info
    else:
        nodes_to_info = nodes_to_info_marker.args[0]

    return sessionless_manager_logic(
        parser=parser,
        nodes_to_info=nodes_to_info
    )


@pytest.fixture(autouse=True)
def zi(
    request #nodes_to_info=None, parser_builder=None,
):
    nodes_to_info_marker = request.node.get_closest_marker("nodes_to_info")
    parser_marker = request.node.get_closest_marker("parser")
    if parser_marker is None:
        parser = lambda x:x
    else:
        parser = parser_marker.args[0]

    if nodes_to_info_marker is None:
        nodes_to_info = zi_structure.nodes_to_info
    else:
        nodes_to_info = nodes_to_info_marker.args[0]

    return sessionless_manager_logic(
        nodes_to_info=nodes_to_info,
        parser=parser,
    ).construct_nodetree(hide_kernel_prefix=True)


@pytest.fixture
def result_node():
    return ResultNode(
        tree_manager=sessionless_manager_logic(),
        path_segments=(),
        subtree_paths=zi_structure.structure,
        value_structure=zi_get_responses_prop,
        timestamp=0,
    ).zi
