from __future__ import annotations

import pytest

from tests.mock_server_for_testing import get_mocked_node, get_unittest_mocked_node


@pytest.mark.parametrize("method", ["__str__", "__repr__"])
@pytest.mark.asyncio
async def test_metanode_not_raises(method):
    node = await get_unittest_mocked_node({"/a/b/c/d": {}})
    getattr(node, method)()


@pytest.mark.parametrize("method", ["__str__", "__repr__", "__dir__"])
@pytest.mark.asyncio
async def test_result_node_not_raises(method):
    node = await get_mocked_node({"/a/b": {}})
    result = await node()
    getattr(result, method)()


@pytest.mark.parametrize("method", ["__str__", "__repr__", "__dir__"])
@pytest.mark.asyncio
async def test_node_not_raises(method):
    node = await get_unittest_mocked_node({"/a/in/c/d": {}})
    getattr(node, method)()


@pytest.mark.parametrize("method", ["__str__", "__repr__", "__dir__"])
@pytest.mark.asyncio
async def test_nodeinfo_not_raises(method):
    node = await get_unittest_mocked_node(
        {
            "/a": {
                "Node": "/a/b",
                "Description": "abcde",
                "Properties": "Read, Write, Setting",
                "Type": "Integer (enumerated)",
                "Unit": "V",
                "Options": {"1": "Sync", "2": "Alive"},
            },
        },
    )
    info = node.a.node_info
    getattr(info, method)()
