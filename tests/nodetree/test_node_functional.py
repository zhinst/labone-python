from __future__ import annotations

import pickle
import typing as t
import warnings
from enum import Enum
from io import BytesIO

import pytest

if t.TYPE_CHECKING:
    from labone.core.value import AnnotatedValue
from labone.nodetree.enum import _get_enum
from labone.nodetree.errors import LabOneInvalidPathError
from tests.mock_server_for_testing import get_mocked_node, get_unittest_mocked_node


@pytest.mark.asyncio
async def test_node_dot_subpathing():
    node = await get_unittest_mocked_node({"/a/b/c/d": {}})

    assert node.a.b.c.d.path == "/a/b/c/d"


@pytest.mark.asyncio
async def test_node_bracket_subpathing():
    node = await get_unittest_mocked_node({"/a/b/c/d": {}})

    assert node["a"]["b"]["c"]["d"].path == "/a/b/c/d"


@pytest.mark.asyncio
async def test_node_bracket_path_subpathing():
    node = await get_unittest_mocked_node({"/a/b/c/d": {}})

    assert node["a/b/c/d"].path == "/a/b/c/d"


@pytest.mark.asyncio
async def test_node_bracket_number_subpathing():
    node = await get_unittest_mocked_node({"/0": {}})
    assert node[0].path == "/0"


@pytest.mark.asyncio
async def test_node_bracket_wildcard_subpathing():
    node = await get_unittest_mocked_node({"/a/b": {}})
    assert node["*"].b.path == "/*/b"


@pytest.mark.asyncio
async def test_subpathing_invalid_path_raises():
    node = await get_unittest_mocked_node({"/a": {}})
    with pytest.raises(LabOneInvalidPathError):
        node.b  # noqa: B018


@pytest.mark.asyncio
async def test_subpathing_too_deep_path_raises():
    node = await get_unittest_mocked_node({"/a": {}})
    with pytest.raises(LabOneInvalidPathError):
        node.a.b  # noqa: B018


@pytest.mark.asyncio
async def test_subpathing_too_deep_path_long_brackets_raises():
    node = await get_unittest_mocked_node({"/a": {}})
    with pytest.raises(LabOneInvalidPathError):
        node["a/b"]


@pytest.mark.asyncio
async def test_access_same_node_repeatedly():
    node = await get_unittest_mocked_node({"/a/b": {}})
    for _ in range(10):
        node.a.b  # noqa: B018


@pytest.mark.asyncio
async def test_hide_kernel_prefix():
    node = await get_unittest_mocked_node({"/a/b": {}}, hide_kernel_prefix=True)
    assert node.path == "/a"


@pytest.mark.asyncio
async def test_dont_hide_kernel_prefix():
    node = await get_unittest_mocked_node({"/a/b": {}}, hide_kernel_prefix=False)
    assert node.path == "/"


@pytest.mark.asyncio
async def test_root_property_plain():
    node = await get_unittest_mocked_node({"/a/b": {}}, hide_kernel_prefix=False)
    assert node.a.b.root == node


@pytest.mark.asyncio
async def test_root_property_hide_kernel_prefix():
    node = await get_unittest_mocked_node({"/a/b": {}}, hide_kernel_prefix=True)
    assert node.b.root == node


@pytest.mark.parametrize(
    "paths",
    [
        set(),
        {"/a"},
        {"/a", "/c", "/b", "/d"},
    ],
)
@pytest.mark.asyncio
async def test_iterating_over_node(paths):
    node = await get_unittest_mocked_node({path: {} for path in paths})
    assert {subnode.path for subnode in node} == paths


@pytest.mark.asyncio
async def test_iterating_over_node_sorted():
    paths = {"/a", "/c", "/b", "/d"}
    node = await get_unittest_mocked_node({path: {} for path in paths})
    assert [subnode.path for subnode in node] == sorted(paths)


@pytest.mark.asyncio
async def test_length_of_node():
    node = await get_unittest_mocked_node({"/a/b": {}, "/a/c": {}})
    assert len(node) == 1
    assert len(node.a) == 2


@pytest.mark.asyncio
async def test_contains_next_segment():
    node = await get_unittest_mocked_node({"/a/b": {}, "/a/c": {}})
    assert "a" in node
    assert "b" in node.a
    assert "c" in node.a
    assert "d" not in node.a


@pytest.mark.asyncio
async def test_contains_subnode():
    node = await get_unittest_mocked_node({"/a": {}, "/c/d": {}})
    assert node.a in node
    assert node.c in node


@pytest.mark.asyncio
async def test_node_does_not_contain_itself():
    node = await get_unittest_mocked_node({"/a/b": {}})
    assert node not in node  # noqa: PLR0124


@pytest.mark.asyncio
async def test_node_does_not_contain_deeper_child():
    node = await get_unittest_mocked_node({"/a/b": {}})
    assert node.a.b not in node


@pytest.mark.asyncio
async def test_node_does_not_contain_other_path():
    node = await get_unittest_mocked_node({"/a/b": {}, "/c/d": {}})
    assert node.c.d not in node.a


@pytest.mark.asyncio
async def test_enum_parsing():
    node = await get_mocked_node(
        {"/a": {"Options": {"0": "off"}, "Type": "Integer (enumerated)"}},
    )
    await node.a(0)
    value = (await node.a()).value

    assert isinstance(value, Enum)
    assert value == 0
    assert value.name == "off"


@pytest.mark.asyncio
async def test_no_enum_parsing_non_enum_nodes():
    node = await get_mocked_node({"/a": {}})
    await node.a(0)
    value = (await node.a()).value

    assert not isinstance(value, Enum)
    assert value == 0


@pytest.mark.asyncio
async def test_enum_parsing_in_subscriptions():
    node = await get_mocked_node(
        {
            "/a": {
                "Options": {"0": "off"},
                "Type": "Integer (enumerated)",
            },
        },
    )
    queue = await node.a.subscribe()
    await node.a(0)
    value = (await queue.get()).value

    assert isinstance(value, Enum)
    assert value == 0
    assert value.name == "off"


@pytest.mark.asyncio
async def test_custom_parser():
    def custom_parser(value: AnnotatedValue) -> AnnotatedValue:
        value.value = value.value * 100
        return value

    node = await get_mocked_node({"/a": {}}, custom_parser=custom_parser)
    await node.a(5)
    assert (await node.a()).value == 500


@pytest.mark.asyncio
async def test_custom_parser_in_subscriptions():
    def custom_parser(value: AnnotatedValue) -> AnnotatedValue:
        value.value = value.value * 100
        return value

    node = await get_mocked_node({"/a": {}}, custom_parser=custom_parser)

    queue = await node.a.subscribe()
    await node.a(5)
    assert (await queue.get()).value == 500


@pytest.mark.asyncio
async def test_comparable():
    paths = {"/a/d", "/a/e", "/a/b", "/a/c", "/a/f"}
    node = await get_unittest_mocked_node({path: {} for path in paths})

    # same comparison behavior as corresponding paths
    assert sorted(paths) == sorted([node[p].path for p in paths])


@pytest.mark.asyncio
async def test_hashing():
    node = await get_unittest_mocked_node({"/a/b": {}, "/c/d": {}, "/e/f": {}})
    nodes = [node.a.b, node.c.d, node.e.f]

    # lossless hashing
    assert sorted(set(nodes)) == sorted(nodes)


def test_pickle_enum():
    enum_value = _get_enum(
        path="/a",
        info={
            "Options": {"0": "off"},
            "Type": "Integer (enumerated)",
        },
    )(0)
    buffer = BytesIO()
    pickle.dump(enum_value, buffer)

    buffer.seek(0)
    unpickled_obj = pickle.load(buffer)  # noqa: S301

    assert unpickled_obj == enum_value


@pytest.mark.asyncio
async def test_keyword_paths():
    node = await get_mocked_node({"/with/in/try": {}})
    assert node.with_.in_.try_.path == "/with/in/try"


@pytest.mark.asyncio
async def test_adding_nodes_manually_with_info():
    node = await get_unittest_mocked_node({"/a/b": {}})
    node.tree_manager.add_nodes_with_info({"/a/c": {}})
    assert node.a.c.path == "/a/c"  # accessable


@pytest.mark.asyncio
async def test_adding_multiple_nodes_manually_with_info():
    node = await get_unittest_mocked_node({"/a/b": {}})
    node.tree_manager.add_nodes_with_info({"/a/c": {}, "/a/d": {}})
    assert node.a.c.path == "/a/c"
    assert node.a.d.path == "/a/d"  # accessable


@pytest.mark.asyncio
async def test_adding_nodes_manually():
    node = await get_unittest_mocked_node({"/a/b": {}})
    node.tree_manager.add_nodes(["/a/c"])
    assert node.a.c.path == "/a/c"  # accessable


@pytest.mark.asyncio
async def test_adding_nodes_manually_hidden_prefix_change():
    node = await get_mocked_node({"/common_prefix/b": {}}, hide_kernel_prefix=True)
    node = node.root
    subnode_via_hidden_prefix = node.b

    # once no commen first prefix exists, the access via hidden prefix
    # is not possible anymore
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        node.tree_manager.add_nodes(["/other_prefix/c"])
    node = node.root
    subnode_via_shown_prefix = node.common_prefix.b

    assert subnode_via_hidden_prefix == subnode_via_shown_prefix


@pytest.mark.asyncio
async def test_adding_nodes_manually_hidden_prefix_does_only_change_if_required():
    node = await get_unittest_mocked_node(
        {"/common_prefix/b": {}},
        hide_kernel_prefix=True,
    )
    subnode_via_hidden_prefix = node.b

    # common first prefix still exists, so the access via hidden prefix
    # is still possible
    node.tree_manager.add_nodes(["/common_prefix/c"])
    assert subnode_via_hidden_prefix == node.b
