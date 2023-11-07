import pickle
from enum import Enum
from io import BytesIO

import pytest
from labone.core import AnnotatedValue
from labone.nodetree.enum import (
    _get_enum,
    get_default_enum_parser,
)
from labone.nodetree.node import NodeInfo

from tests.nodetree.conftest import (
    device_structure,
    zi_structure,
)
from tests.nodetree.test_node import get_serverless_tree


class TestNodeInfo:
    @staticmethod
    def test_node_info():
        zi = get_serverless_tree()
        path = zi.debug.level.path
        info = NodeInfo(zi.tree_manager.path_to_info[path])
        assert isinstance(info, NodeInfo)

        # test callablity of methods
        str(info)
        repr(info)
        assert "writable" in info
        assert "Unit" in info
        assert isinstance(info.Unit, str)
        assert isinstance(info.readable, bool)
        assert isinstance(info.writable, bool)
        assert isinstance(info.is_vector, bool)
        assert isinstance(info.is_setting, bool)
        assert isinstance(info.description, str)
        assert isinstance(info.type, str)
        assert isinstance(info.unit, str)
        assert isinstance(info.options, dict)
        assert info == info  # noqa: PLR0124
        assert info != 42
        assert hash(info) == hash(info)


def test_parse_enum2():
    parser = get_default_enum_parser(zi_structure.nodes_to_info)

    # this node is enumerated
    parsed = parser(
        AnnotatedValue(path="/zi/mds/groups/0/status", value=1),
    )
    assert isinstance(parsed.value, Enum)
    assert parsed.value.value == 1  # IntEnum equality
    assert parsed.value.name == "Sync"

    # this node is not enumerated
    parsed = parser(
        AnnotatedValue(path="/zi/mds/groups/0/locked", value=1),
    )
    assert not isinstance(parsed.value, Enum)
    assert parsed.value == 1

    # this node will not be parsed, because it is not found in the infos
    ann = AnnotatedValue(path="invalid/node", value=1)
    assert ann == parser(ann)


def test_get_enum():
    # this node is enumerated
    enum = _get_enum(
        path="/zi/mds/groups/0/status",
        info=zi_structure.nodes_to_info["/zi/mds/groups/0/status"],
    )
    assert issubclass(enum, Enum)

    # enumerated with "" at keywords
    enum = _get_enum(
        path="/dev12084/demods/7/adcselect",
        info=device_structure.nodes_to_info["/dev12084/demods/7/adcselect"],
    )
    assert issubclass(enum, Enum)


def test_get_enum_fail():
    # this node is not enumerated
    assert (
        _get_enum(
            path="/zi/mds/groups/0/locked",
            info=zi_structure.nodes_to_info["/zi/mds/groups/0/locked"],
        )
        is None
    )

    with pytest.raises(KeyError):
        _get_enum(path="invalid/node", info=zi_structure.nodes_to_info["invalid/node"])


def test_pickle_enum():
    enum_value = _get_enum(
        path="/zi/mds/groups/0/status",
        info=zi_structure.nodes_to_info["/zi/mds/groups/0/status"],
    )(1)
    buffer = BytesIO()
    pickle.dump(enum_value, buffer)

    buffer.seek(0)
    unpickled_obj = pickle.load(buffer)  # noqa: S301

    assert unpickled_obj == enum_value


def test_enum_parser_non_existing_node():
    parser = get_default_enum_parser(zi_structure.nodes_to_info)
    result = parser(AnnotatedValue(path="/zi/invalid/node", value=1))
    assert isinstance(result.value, int)
    assert result.value == 1


def test_enum_parser_invalid_value():
    parser = get_default_enum_parser(zi_structure.nodes_to_info)
    result = parser(AnnotatedValue(path="/zi/debug/level", value="67"))
    assert isinstance(result.value, str)
    assert result.value == "67"
