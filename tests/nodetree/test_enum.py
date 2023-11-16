import pickle
import typing as t
from enum import Enum, IntEnum
from io import BytesIO
from unittest.mock import ANY, patch

import pytest
from labone.core import AnnotatedValue

if t.TYPE_CHECKING:
    from labone.core.session import NodeInfo as NodeInfoType
from labone.nodetree.enum import (
    NodeEnum,
    NodeEnumMeta,
    _get_enum,
    get_default_enum_parser,
)


def test_node_enum_meta_new():
    with patch("labone.nodetree.enum.NodeEnum.__init__", autospec=True) as mock_init:
        NodeEnumMeta(2, "name", {"a": 1, "b": 2}, "labone.nodetree.enum.NodeEnum")
        mock_init.assert_called_with(ANY, 2)


def test_node_enum_reduce_ex():
    node_enum = NodeEnum(
        "name",
        {"a": 1, "b": 2},
        module="labone.nodetree.enum.NodeEnum",
    )
    instance = node_enum(2)
    assert instance.__reduce_ex__(_=None) == (
        NodeEnumMeta,
        (2, "name", {"a": 1, "b": 2}, "labone.nodetree.enum.NodeEnum"),
    )


def test_get_enum_no_options():
    node_info: NodeInfoType = {
        "Node": "/ZI/DEBUG/LEVEL",
        "Description": "Set the logging level (amount of detail)"
        " of the LabOne Data Server.",
        "Properties": "Read, Write, Setting",
        "Type": "Integer (enumerated)",
        "Unit": "None",
    }

    assert _get_enum(path="/ZI/DEBUG/LEVEL", info=node_info) is None


def test_get_enum(zi_structure):
    """In this case, the Options are given with extra '' but not just plain."""
    node_info = zi_structure.nodes_to_info["/zi/debug/level"]
    enum = _get_enum(path="/zi/debug/level", info=node_info)

    assert enum.__name__ == "/zi/debug/level"
    assert enum.__module__ == "labone.nodetree.enum"

    expected_options = ["trace", "debug", "info", "status", "warning", "error", "fatal"]
    for i, e in enumerate(expected_options):
        assert enum(i).name == e


def test_get_enum_plain_options(zi_structure):
    """In this case, the Options are not given with extra '' but just plain."""
    path = "/zi/mds/groups/0/status"
    node_info = zi_structure.nodes_to_info[path]
    enum = _get_enum(path=path, info=node_info)

    assert enum.__name__ == path
    assert enum.__module__ == "labone.nodetree.enum"

    expected_options = [
        "Error. An error occurred in the synchronization process.",
        "New",
        "Sync",
        "Alive",
    ]
    for i, e in enumerate(expected_options):
        assert enum(i - 1).name == e


@pytest.fixture()
def default_enum_parser(zi_structure):
    return get_default_enum_parser(zi_structure.nodes_to_info)


def test_default_parser(default_enum_parser):
    parsed = default_enum_parser(
        AnnotatedValue(path="/zi/mds/groups/0/status", value=1),
    )
    assert isinstance(parsed.value, IntEnum)
    assert parsed.value.value == 1  # IntEnum equality
    assert parsed.value.name == "Sync"


def test_default_parser_value_not_in_enum(default_enum_parser):
    parsed = default_enum_parser(
        AnnotatedValue(path="/zi/mds/groups/0/status", value=42),
    )
    assert not isinstance(parsed.value, Enum)
    assert parsed.value == 42


def test_default_parser_not_enum(default_enum_parser):
    # this node is not enumerated
    parsed = default_enum_parser(
        AnnotatedValue(path="/zi/mds/groups/0/locked", value=1),
    )
    assert not isinstance(parsed.value, Enum)
    assert parsed.value == 1


def test_default_parser_invalid_path(default_enum_parser):
    # this node will not be parsed, because it is not found in the infos
    ann = AnnotatedValue(path="invalid/node", value=1)
    assert ann == default_enum_parser(ann)


def test_pickle_enum(zi_structure):
    enum_value = _get_enum(
        path="/zi/mds/groups/0/status",
        info=zi_structure.nodes_to_info["/zi/mds/groups/0/status"],
    )(1)
    buffer = BytesIO()
    pickle.dump(enum_value, buffer)

    buffer.seek(0)
    unpickled_obj = pickle.load(buffer)  # noqa: S301

    assert unpickled_obj == enum_value
