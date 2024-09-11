"""
In the current implementation, the node info is not used for
any functionality. Nethertheless, it may be useful for
customers.
"""

from __future__ import annotations

import pytest

from labone.node_info import NodeInfo, OptionInfo
from tests.mock_server_for_testing import get_unittest_mocked_node


@pytest.mark.asyncio
async def test_node_info_accessable():
    node = await get_unittest_mocked_node(
        {
            "/a/b": {
                "Node": "/a/b",
                "Description": "abcde",
                "Properties": "Read, Write, Setting",
                "Type": "Integer (enumerated)",
                "Unit": "V",
                "Options": {"1": "Sync", "2": "Alive"},
            },
        },
    )
    node.a.b.node_info  # noqa: B018 # no error


@pytest.mark.asyncio
async def test_node_info_attribute_redirects_to_node_info():
    plain_info = {
        "Node": "/a/b",
        "Description": "abcde",
        "Properties": "Read, Write, Setting",
        "Type": "Integer (enumerated)",
        "Unit": "V",
        "Options": {"1": "Sync", "2": "Alive"},
    }
    node = await get_unittest_mocked_node({"/a/b": plain_info})
    assert node.a.b.node_info.as_dict == NodeInfo(plain_info).as_dict


@pytest.mark.parametrize(
    ("attribute", "expected"),
    [
        ("path", "/a/b"),
        ("description", "abcde"),
        ("properties", "Read, Write, Setting"),
        ("type", "Integer (enumerated)"),
        ("unit", "V"),
    ],
)
@pytest.mark.asyncio
async def test_node_info_attributes(attribute, expected):
    info = NodeInfo(
        {
            "Node": "/a/b",
            "Description": "abcde",
            "Properties": "Read, Write, Setting",
            "Type": "Integer (enumerated)",
            "Unit": "V",
            "Options": {"1": "Sync", "2": "Alive"},
        },
    )
    assert getattr(info, attribute) == expected


@pytest.mark.parametrize(
    ("plain_options", "parsed_options"),
    [
        (
            {"1": "Sync", "2": "Alive"},
            {
                1: OptionInfo(enum="Sync", description=""),
                2: OptionInfo(enum="Alive", description=""),
            },
        ),
        (
            {
                "1": '"Sync": Currently Synchronizing',
                "2": '"Alive": Device is reachable.',
            },
            (
                {
                    1: OptionInfo(enum="Sync", description="Currently Synchronizing"),
                    2: OptionInfo(enum="Alive", description="Device is reachable."),
                }
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_options(plain_options, parsed_options):
    info = NodeInfo(
        {
            "Node": "/a/b",
            "Description": "abcde",
            "Properties": "Read, Write, Setting",
            "Type": "Integer (enumerated)",
            "Unit": "V",
            "Options": plain_options,
        },
    )
    assert info.options == parsed_options


@pytest.mark.parametrize(
    ("attribute", "expected"),
    [
        ("readable", True),
        ("writable", True),
        ("is_setting", True),
        ("is_vector", False),
        ("path", "/a/b"),
    ],
)
@pytest.mark.asyncio
async def test_node_info_emergent_attributes(attribute, expected):
    info = NodeInfo(
        {
            "Node": "/a/b",
            "Description": "abcde",
            "Properties": "Read, Write, Setting",
            "Type": "Integer (enumerated)",
            "Unit": "V",
            "Options": {"1": "Sync", "2": "Alive"},
        },
    )
    assert getattr(info, attribute) == expected


@pytest.mark.parametrize(
    ("attribute", "expected"),
    [
        ("readable", False),
        ("writable", False),
        ("is_setting", False),
        ("is_vector", True),
        ("path", "/a/b"),
    ],
)
@pytest.mark.asyncio
async def test_node_info_emergent_attributes_case2(attribute, expected):
    info = NodeInfo(
        {
            "Node": "/a/b",
            "Description": "abcde",
            "Properties": "",
            "Type": "ZIVectorData",
            "Unit": "V",
        },
    )
    assert getattr(info, attribute) == expected


@pytest.mark.asyncio
async def test_empty_node_info_representable():
    info = NodeInfo({})
    repr(info)


@pytest.mark.asyncio
async def test_empty_node_info_stringifyable():
    info = NodeInfo({})
    str(info)


@pytest.mark.asyncio
async def test_partial_infos_partially_useable():
    info = NodeInfo({"Type": "ZIVectorData"})
    assert info.type == "ZIVectorData"  # no error


@pytest.mark.asyncio
async def test_incomplete_info_raises():
    with pytest.raises(KeyError):
        NodeInfo({}).type  # noqa: B018


@pytest.mark.asyncio
async def test_default_info_readable_writeable():
    info = NodeInfo(NodeInfo.plain_default_info(path="/a/b"))
    assert info.readable is True
    assert info.writable is True
