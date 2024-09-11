import pytest

from labone.core.value import AnnotatedValue
from labone.nodetree.errors import LabOneInvalidPathError
from tests.mock_server_for_testing import get_mocked_node


@pytest.mark.asyncio
async def test_partial_get_result_node():
    node = await get_mocked_node(
        {"/a/b/c/d": {}},
    )
    response = await node.a.b()
    assert response.path_segments == ()
    response.a.b.c.d  # noqa: B018 # path valid


@pytest.mark.asyncio
async def test_partial_get_result_node_multiple_subpaths():
    node = await get_mocked_node(
        {
            "/a/c": {},
            "/a/d": {},
        },
    )

    response = await node.a()
    assert response.path_segments == ()
    response.a.c  # noqa: B018 # path valid
    response.a.d  # noqa: B018 # path valid


@pytest.mark.asyncio
async def test_partial_get_result_node_long_bracket_subpathing():
    node = await get_mocked_node(
        {"/a/b/c": {}},
    )
    response = await node()
    response["a/b/c"]  # path valid


@pytest.mark.asyncio
async def test_partial_get_result_node_bracket_subpathing():
    node = await get_mocked_node(
        {"/a/b/c": {}},
    )
    response = await node()
    response["a"]["b"]["c"]  # path valid


@pytest.mark.asyncio
async def test_partial_get_result_node_bracket_integer_subpathing():
    node = await get_mocked_node({"/0": {}})
    response = await node()
    response[0]  # path valid


@pytest.mark.asyncio
async def test_partial_get_result_node_wrong_path_raises():
    node = await get_mocked_node({"/a": {}})
    response = await node()
    with pytest.raises(LabOneInvalidPathError):
        response.b  # noqa: B018


@pytest.mark.asyncio
async def test_partial_get_result_node_too_long_path_raises():
    node = await get_mocked_node({"/a": {}})
    response = await node()
    with pytest.raises(AttributeError):  # will be a AnnotatedValue Attribute error
        response.a.b  # noqa: B018


@pytest.mark.asyncio
async def test_partial_get_result_node_too_long_bracket_path_raises():
    node = await get_mocked_node({"/a": {}})
    response = await node()
    with pytest.raises(LabOneInvalidPathError):
        response["a/b"]


@pytest.mark.asyncio
async def test_partial_get_result_node_iterate_through_leaves():
    paths = {"/a/b/c", "/a/b/d", "/a/b/e"}
    node = await get_mocked_node(
        {p: {} for p in paths},
    )
    response = await node.a()
    assert paths == {leaf.path for leaf in response.results()}


@pytest.mark.asyncio
async def test_partial_get_result_node_iterate_through_leaves_partial_scope():
    paths = {"/a/b/c", "/a/b/d", "/x/y"}
    node = await get_mocked_node(
        {p: {} for p in paths},
    )
    response = await node()

    # results shall only give results agreing to the current path prefix
    assert {"/x/y"} == {leaf.path for leaf in response.x.results()}


@pytest.mark.asyncio
async def test_partial_get_result_node_values_as_leafs():
    node = await get_mocked_node({"/a/b": {}})
    result = await node()
    assert isinstance(result.a.b, AnnotatedValue)


@pytest.mark.asyncio
async def test_partial_get_result_node_contains_next_segment():
    node = await get_mocked_node({"/a/b": {}, "/a/c": {}})
    result = await node()
    assert "a" in result
    assert "b" in result.a
    assert "c" in result.a
    assert "d" not in result.a


@pytest.mark.asyncio
async def test_partial_get_result_node_contains_subnode():
    node = await get_mocked_node({"/a/b": {}})
    result = await node()
    assert result.a in result


@pytest.mark.asyncio
async def test_partial_get_result_node_contains_value_at_leaf():
    node = await get_mocked_node({"/a": {}})
    result = await node()
    assert result.a in result


@pytest.mark.asyncio
async def test_partial_get_result_node_only_matches_accessable():
    node = await get_mocked_node({"/a/b/c": {}, "/a/x/c": {}})
    response = await node.a.b()  # different pattern
    with pytest.raises(LabOneInvalidPathError):
        response.a.x  # noqa: B018


@pytest.mark.asyncio
async def test_partial_get_result_node_only_access_same_node_repeatedly():
    node = await get_mocked_node({"/a/b": {}})
    response = await node()
    for _ in range(10):
        response.a.b  # noqa: B018


@pytest.mark.asyncio
async def test_wildcard_get_result_node_basic_behavior():
    node = await get_mocked_node({"/a/b/c": {}})
    response = await node.a["*"].c()
    assert response.path_segments == ()
    response.a.b.c  # noqa: B018


@pytest.mark.asyncio
async def test_wildcard_get_result_node_hide_prefix():
    node = await get_mocked_node(
        {"/a/b/c": {}},
        hide_kernel_prefix=True,
    )
    response = await node["*"].c()
    assert response.path_segments == ("a",)
    response.b.c  # noqa: B018


@pytest.mark.asyncio
async def test_wildcard_get_result_node_multiple_matches():
    node = await get_mocked_node({"/a/b/c": {}, "/a/x/c": {}})
    response = await node.a["*"].c()
    response.a.b.c  # noqa: B018
    response.a.x.c  # noqa: B018


@pytest.mark.asyncio
async def test_wildcard_get_result_node_only_matches_accessable():
    node = await get_mocked_node({"/a/b/c": {}})
    response = await node.a["*"].d()  # different pattern
    with pytest.raises(LabOneInvalidPathError):
        response.a.b.c  # noqa: B018
