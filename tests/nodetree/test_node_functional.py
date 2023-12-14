from __future__ import annotations

import pickle
from enum import Enum
from io import BytesIO

import pytest
from labone.core.value import AnnotatedValue
from labone.nodetree.enum import _get_enum
from labone.nodetree.errors import LabOneInvalidPathError

from tests.mock_server_for_testing import get_mocked_node, get_unittest_mocked_node


@pytest.mark.asyncio()
async def test_node_dot_subpathing():
    node = await get_unittest_mocked_node({"/a/b/c/d": {}})

    assert node.a.b.c.d.path == "/a/b/c/d"


@pytest.mark.asyncio()
async def test_node_bracket_subpathing():
    node = await get_unittest_mocked_node({"/a/b/c/d": {}})

    assert node["a"]["b"]["c"]["d"].path == "/a/b/c/d"


@pytest.mark.asyncio()
async def test_node_bracket_path_subpathing():
    node = await get_unittest_mocked_node({"/a/b/c/d": {}})

    assert node["a/b/c/d"].path == "/a/b/c/d"


@pytest.mark.asyncio()
async def test_node_bracket_number_subpathing():
    node = await get_unittest_mocked_node({"/0": {}})
    assert node[0].path == "/0"


@pytest.mark.asyncio()
async def test_node_bracket_wildcard_subpathing():
    node = await get_unittest_mocked_node({"/a/b": {}})
    assert node["*"].b.path == "/*/b"


@pytest.mark.asyncio()
async def test_subpathing_invalid_path_raises():
    node = await get_unittest_mocked_node({"/a": {}})
    with pytest.raises(LabOneInvalidPathError):
        node.b  # noqa: B018


@pytest.mark.asyncio()
async def test_subpathing_too_deep_path_raises():
    node = await get_unittest_mocked_node({"/a": {}})
    with pytest.raises(LabOneInvalidPathError):
        node.a.b  # noqa: B018


@pytest.mark.asyncio()
async def test_subpathing_too_deep_path_long_brackets_raises():
    node = await get_unittest_mocked_node({"/a": {}})
    with pytest.raises(LabOneInvalidPathError):
        node["a/b"]


@pytest.mark.asyncio()
async def test_hide_kernel_prefix():
    node = await get_unittest_mocked_node({"/a/b": {}}, hide_kernel_prefix=True)
    assert node.path == "/a"


@pytest.mark.asyncio()
async def test_dont_hide_kernel_prefix():
    node = await get_unittest_mocked_node({"/a/b": {}}, hide_kernel_prefix=False)
    assert node.path == "/"


@pytest.mark.asyncio()
async def test_root_property_plain():
    node = await get_unittest_mocked_node({"/a/b": {}}, hide_kernel_prefix=False)
    assert node.a.b.root == node


@pytest.mark.asyncio()
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
@pytest.mark.asyncio()
async def test_iterating_over_node(paths):
    node = await get_unittest_mocked_node({path: {} for path in paths})
    assert {subnode.path for subnode in node} == paths


@pytest.mark.asyncio()
async def test_iterating_over_node_sorted():
    paths = {"/a", "/c", "/b", "/d"}
    node = await get_unittest_mocked_node({path: {} for path in paths})
    assert [subnode.path for subnode in node] == sorted(paths)


@pytest.mark.asyncio()
async def test_length_of_node():
    node = await get_unittest_mocked_node({"/a/b": {}, "/a/c": {}})
    assert len(node) == 1
    assert len(node.a) == 2


@pytest.mark.asyncio()
async def test_contains_next_segment():
    node = await get_unittest_mocked_node({"/a/b": {}, "/a/c": {}})
    assert "a" in node
    assert "b" in node.a
    assert "c" in node.a
    assert "d" not in node.a


@pytest.mark.asyncio()
async def test_contains_subnode():
    node = await get_unittest_mocked_node({"/a/b": {}, "/c/d": {}})
    assert node.a in node
    assert node.c.d not in node.a


@pytest.mark.asyncio()
async def test_enum_parsing():
    node = await get_mocked_node(
        {"/a": {"Options": {"0": "off"}, "Type": "Integer (enumerated)"}},
    )
    await node.a(0)
    value = (await node.a()).value

    assert isinstance(value, Enum)
    assert value == 0
    assert value.name == "off"


@pytest.mark.asyncio()
async def test_no_enum_parsing_non_enum_nodes():
    node = await get_mocked_node({"/a": {}})
    await node.a(0)
    value = (await node.a()).value

    assert not isinstance(value, Enum)
    assert value == 0


@pytest.mark.asyncio()
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


@pytest.mark.asyncio()
async def test_custom_parser():
    def custom_parser(value: AnnotatedValue) -> AnnotatedValue:
        value.value = value.value * 100
        return value

    node = await get_mocked_node({"/a": {}}, custom_parser=custom_parser)
    await node.a(5)
    assert (await node.a()).value == 500


@pytest.mark.asyncio()
async def test_custom_parser_in_subscriptions():
    def custom_parser(value: AnnotatedValue) -> AnnotatedValue:
        value.value = value.value * 100
        return value

    node = await get_mocked_node({"/a": {}}, custom_parser=custom_parser)

    queue = await node.a.subscribe()
    await node.a(5)
    assert (await queue.get()).value == 500


@pytest.mark.asyncio()
async def test_comparable():
    paths = {"/a/d", "/a/e", "/a/b", "/a/c", "/a/f"}
    node = await get_unittest_mocked_node({path: {} for path in paths})

    # same comparison behavior as corresponding paths
    assert sorted(paths) == sorted([node[p].path for p in paths])


@pytest.mark.asyncio()
async def test_hashing():
    node = await get_unittest_mocked_node({"/a/b": {}, "/c/d": {}, "/e/f": {}})
    nodes = [node.a.b, node.c.d, node.e.f]

    # lossless hashing
    assert sorted(set(nodes)) == sorted(nodes)


@pytest.mark.asyncio()
async def test_partial_get_result_node():
    node = await get_mocked_node(
        {"/a/b/c/d/e/f/g": {}},
    )

    response = await node.a.b()
    assert response.path_segments == ("a", "b")
    response.c.d.e.f.g  # noqa: B018 # path valid


@pytest.mark.asyncio()
async def test_partial_get_result_node_multiple_subpaths():
    node = await get_mocked_node(
        {
            "/a/c": {},
            "/a/d": {},
        },
    )

    response = await node.a()
    assert response.path_segments == ("a",)
    response.c  # noqa: B018 # path valid
    response.d  # noqa: B018 # path valid


@pytest.mark.asyncio()
async def test_partial_get_result_node_long_bracket_subpathing():
    node = await get_mocked_node(
        {"/a/b/c": {}},
    )
    response = await node()
    response["a/b/c"]  # path valid


@pytest.mark.asyncio()
async def test_partial_get_result_node_bracket_subpathing():
    node = await get_mocked_node(
        {"/a/b/c": {}},
    )
    response = await node()
    response["a"]["b"]["c"]  # path valid


@pytest.mark.asyncio()
async def test_partial_get_result_node_bracket_integer_subpathing():
    node = await get_mocked_node(
        {"/0": {}},
    )
    response = await node()
    response[0]  # path valid


@pytest.mark.asyncio()
async def test_partial_get_result_node_wrong_path_raises():
    node = await get_mocked_node({"/a": {}})
    response = await node()
    with pytest.raises(LabOneInvalidPathError):
        response.b  # noqa: B018


@pytest.mark.asyncio()
async def test_partial_get_result_node_too_long_path_raises():
    node = await get_mocked_node({"/a": {}})
    response = await node()
    with pytest.raises(AttributeError):  # will be a AnnotatedValue Attribute error
        response.a.b  # noqa: B018


@pytest.mark.asyncio()
async def test_partial_get_result_node_too_long_bracket_path_raises():
    node = await get_mocked_node({"/a": {}})
    response = await node()
    with pytest.raises(LabOneInvalidPathError):
        response["a/b"]


@pytest.mark.asyncio()
async def test_partial_get_result_node_iterate_through_leaves():
    paths = {"/a/b/c", "/a/b/d", "/a/b/e"}
    node = await get_mocked_node(
        {p: {} for p in paths},
    )
    response = await node.a()
    assert paths == {leaf.path for leaf in response.results()}


@pytest.mark.asyncio()
async def test_partial_get_result_node_iterate_through_leaves_partial_scope():
    paths = {"/a/b/c", "/a/b/d", "/x/y"}
    node = await get_mocked_node(
        {p: {} for p in paths},
    )
    response = await node()

    # results shall only give results agreing to the current path prefix
    assert {"/x/y"} == {leaf.path for leaf in response.x.results()}


@pytest.mark.asyncio()
async def test_partial_get_result_node_values_as_leafs():
    node = await get_mocked_node({"/a/b": {}})
    result = await node()
    assert isinstance(result.a.b, AnnotatedValue)


@pytest.mark.asyncio()
async def test_partial_get_result_node_contains_next_segment():
    node = await get_mocked_node({"/a/b": {}, "/a/c": {}})
    result = await node()
    assert "a" in result
    assert "b" in result.a
    assert "c" in result.a
    assert "d" not in result.a


@pytest.mark.asyncio()
async def test_partial_get_result_node_contains_subnode():
    node = await get_mocked_node({"/a/b": {}})
    result = await node()
    assert result.a in result


@pytest.mark.asyncio()
async def test_partial_get_result_node_contains_value_at_leaf():
    node = await get_mocked_node({"/a": {}})
    result = await node()
    assert result.a in result


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


@pytest.mark.asyncio()
async def test_keyword_paths():
    node = await get_mocked_node({"/with/in/try": {}})
    assert node.with_.in_.try_.path == "/with/in/try"


# Add tests for:
# Result nodes of partial and wildcard nodes are working
# auch in result nodes verschiedene zugriffe . []

# hash and eq for nodes
# ist die node info eigentlich fuer irgendetwas required?
# ist wait for state change im nodetree noch relevant?
# braucht irgendwer die custom parser?

# hashing von result nodes unterschiedlich nach timestamp, ...
