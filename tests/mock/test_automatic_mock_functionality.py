"""Unit Tests for the AutomaticLabOneServer."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pytest

from labone.core.errors import LabOneCoreError

if TYPE_CHECKING:
    from labone.core.helper import LabOneNodePath
from labone.core.value import AnnotatedValue, Value
from labone.mock import AutomaticLabOneServer


async def get_functionality_with_state(state: dict[LabOneNodePath, Value]):
    functionality = AutomaticLabOneServer({path: {} for path in state})
    for path, value in state.items():
        await functionality.set(AnnotatedValue(value=value, path=path))
    return functionality


@pytest.mark.asyncio
async def test_node_info_default_readable():
    functionality = AutomaticLabOneServer({"/a/b": {}})
    await functionality.get("/a/b")


@pytest.mark.asyncio
async def test_node_info_default_writable():
    functionality = AutomaticLabOneServer({"/a/b": {}})
    await functionality.set(AnnotatedValue(path="/a/b", value=1))


async def check_state_agrees_with(
    functionality: AutomaticLabOneServer,
    state: dict[LabOneNodePath, Value],
) -> bool:
    for path, value in state.items():
        if (await functionality.get(path)).value != value:
            return False
    return True


@pytest.mark.asyncio
async def test_remembers_state():
    functionality = AutomaticLabOneServer({"/a/b": {}})
    await functionality.set(AnnotatedValue(value=123, path="/a/b"))
    assert (await functionality.get("/a/b")).value == 123


@pytest.mark.asyncio
async def test_relavtive_path():
    functionality = AutomaticLabOneServer({"/a/b": {}})
    await functionality.set(AnnotatedValue(value=123, path="b"))
    assert (await functionality.get("b")).value == 123


@pytest.mark.asyncio
async def test_state_overwritable():
    functionality = AutomaticLabOneServer({"/a/b": {}})
    await functionality.set(AnnotatedValue(value=123, path="/a/b"))
    await functionality.set(AnnotatedValue(value=456, path="/a/b"))
    assert (await functionality.get("/a/b")).value == 456


@pytest.mark.asyncio
async def test_seperate_state_per_path():
    functionality = AutomaticLabOneServer({"/a/b": {}, "/a/c": {}})
    await functionality.set(AnnotatedValue(value=123, path="/a/b"))
    await functionality.set(AnnotatedValue(value=456, path="/a/c"))
    assert (await functionality.get("/a/b")).value == 123
    assert (await functionality.get("/a/c")).value == 456


@pytest.mark.asyncio
async def test_cannot_get_outside_of_tree_structure():
    functionality = AutomaticLabOneServer({"/a/b": {}})
    with pytest.raises(Exception):  # noqa: B017
        await functionality.get("/a/c")


@pytest.mark.asyncio
async def test_cannot_set_outside_of_tree_structure():
    functionality = AutomaticLabOneServer({"/a/b": {}})
    with pytest.raises(Exception):  # noqa: B017
        await functionality.set(AnnotatedValue(value=123, path="/a/c"))


@pytest.mark.asyncio
async def test_list_nodes_answered_by_tree_structure():
    functionality = AutomaticLabOneServer(
        {"/x": {}, "/x/y": {}, "/v/w/q/a": {}},
    )
    assert set(await functionality.list_nodes("*")) == {"/x", "/x/y", "/v/w/q/a"}


@pytest.mark.parametrize(
    ("path_to_info", "path", "expected"),
    [
        # test option to get all paths with *
        ({}, "*", {}),
        ({"/a/b": {}}, "*", {"/a/b": {}}),
        ({"/a": {}, "/b": {}, "/c/d/e": {}}, "*", {"/a": {}, "/b": {}, "/c/d/e": {}}),
        # if specific path, not necessarily all paths are returned
        ({}, "/a", {}),
        ({"/a/b": {}}, "/c", {}),
        (
            {"/x/y": {}, "/x/z/n": {"Description": "_"}, "/x/z/q/a": {}},
            "/x/z",
            {"/x/z/n": {"Description": "_"}, "/x/z/q/a": {}},
        ),
        ({"/a/b": {}, "/a/c": {}}, "/a", {"/a/b": {}, "/a/c": {}}),
        # a path matches itself
        ({"/a/b": {}}, "/a/b", {"/a/b": {}}),
        # a path does not match itself plus wildcard
        ({"/a/b": {}}, "/a/b/*", {}),
        # test wildcard constillations
        ({"/a/b": {}, "/a/c": {}}, "/*/b", {"/a/b": {}}),
        ({"/a/b": {}, "/a/c": {}}, "/*", {"/a/b": {}, "/a/c": {}}),
    ],
)
@pytest.mark.asyncio
async def test_list_nodes_info(path_to_info, path, expected):
    functionality = AutomaticLabOneServer(path_to_info)
    assert (await functionality.list_nodes_info(path)).keys() == expected.keys()


@pytest.mark.parametrize(
    "path_to_info",
    [
        {},
        {"/a/b": {}},
        {"/a": {}, "/b": {}, "/c/d/e": {}},
        {"/x/y/1": {}, "/x/y/2": {}, "/x/z/n": {}, "/x/z/q/a": {}},
    ],
)
@pytest.mark.parametrize(
    "path",
    [
        "",
        "/a/*",
        "/x/y/*",
        "/x/z/*",
        "/x/*",
        "/*",
    ],
)
@pytest.mark.asyncio
async def test_consistency_list_nodes_vs_list_nodes_info(path_to_info, path):
    functionality = AutomaticLabOneServer(path_to_info)

    assert set((await functionality.list_nodes_info(path)).keys()) == set(
        await functionality.list_nodes(path),
    )


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("/*", {1, 2, 3, 4, 5}),
        ("*", {1, 2, 3, 4, 5}),
        ("/a/b/c", {1}),
        ("/a/b", {1, 2}),
        ("/a", {1, 2, 3, 4}),
        ("/a/x", {3, 4}),
    ],
)
@pytest.mark.asyncio
async def test_get_with_expression(expression, expected):
    functionality = await get_functionality_with_state(
        {
            "/a/b/c": 1,
            "/a/b/d": 2,
            "/a/x": 3,
            "/a/x/y": 4,
            "/b": 5,
        },
    )
    assert {
        ann.value for ann in (await functionality.get_with_expression(expression))
    } == expected


@pytest.mark.parametrize(
    ("expression", "value", "expected_new_state"),
    [
        ("*", 7, {"/a/b/c": 7, "/a/b/d": 7, "/a/x": 7, "/a/x/y": 7, "/b": 7}),
        ("/a/b/c", 7, {"/a/b/c": 7, "/a/b/d": 2, "/a/x": 3, "/a/x/y": 4, "/b": 5}),
        ("/a/b", 7, {"/a/b/c": 7, "/a/b/d": 7, "/a/x": 3, "/a/x/y": 4, "/b": 5}),
        (
            "/a",
            7,
            {
                "/a/b/c": 7,
                "/a/b/d": 7,
                "/a/x": 7,
                "/a/x/y": 7,
                "/b": 5,
            },
        ),
    ],
)
@pytest.mark.asyncio
async def test_set_with_expression(expression, value, expected_new_state):
    functionality = await get_functionality_with_state(
        {
            "/a/b/c": 1,
            "/a/b/d": 2,
            "/a/x": 3,
            "/a/x/y": 4,
            "/b": 5,
        },
    )

    await functionality.set_with_expression(
        AnnotatedValue(value=value, path=expression),
    )

    assert await check_state_agrees_with(functionality, expected_new_state)


@pytest.mark.parametrize(
    "value",
    [
        5,
        6.3,
        "hello",
        b"hello",
        2 + 3j,
    ],
)
@pytest.mark.asyncio
async def test_handling_of_multiple_data_types(value: Value):
    functionality = AutomaticLabOneServer({"/a/b": {}})
    await functionality.set(AnnotatedValue(value=value, path="/a/b"))
    assert (await functionality.get("/a/b")).value == value


@pytest.mark.asyncio
async def test_handling_of_numpy_array():
    functionality = AutomaticLabOneServer({"/a/b": {}})
    value = np.array([1, 2, 3])
    await functionality.set(AnnotatedValue(value=value, path="/a/b"))
    assert np.all((await functionality.get("/a/b")).value == value)


@pytest.mark.asyncio
async def test_timestamps_are_increasing():
    functionality = AutomaticLabOneServer({"/a/b": {}})

    # calling set 10 times to ensure higher probability for wrong order,
    # if timestamps are not increasing
    responses = [
        await functionality.set(AnnotatedValue(value=1, path="/a/b")) for _ in range(10)
    ]

    # calling all functions with timestamp once
    responses.append(await functionality.get("/a/b"))
    responses += await functionality.set_with_expression(
        AnnotatedValue(value=2, path="/a/*"),
    )
    responses += await functionality.get_with_expression("/a/*")

    sorted_by_timestamp = sorted(responses, key=lambda x: x.timestamp)
    assert sorted_by_timestamp == responses


@pytest.mark.asyncio
async def test_cannot_set_readonly_node():
    functionality = AutomaticLabOneServer({"/a/b": {"Properties": "Read"}})
    with pytest.raises(LabOneCoreError):
        await functionality.set(AnnotatedValue(value=1, path="/a/b"))


@pytest.mark.asyncio
async def test_error_when_set_with_expression_no_matches():
    functionality = AutomaticLabOneServer({"/a/b": {}})
    with pytest.raises(LabOneCoreError):
        await functionality.set_with_expression(AnnotatedValue(value=1, path="/b/*"))
