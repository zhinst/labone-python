"""
In the current implementation, the node info is not used for
any functionality. Nethertheless, it may be useful for
customers.
"""


from __future__ import annotations

import pytest
from labone.node_info import OptionInfo

from tests.mock_server_for_testing import get_unittest_mocked_node


@pytest.mark.asyncio()
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
@pytest.mark.asyncio()
async def test_node_info_attributes(attribute, expected):
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
    info = node.a.b.node_info
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
@pytest.mark.asyncio()
async def test_options(plain_options, parsed_options):
    node = await get_unittest_mocked_node(
        {
            "/a/b": {
                "Node": "/a/b",
                "Description": "abcde",
                "Properties": "Read, Write, Setting",
                "Type": "Integer (enumerated)",
                "Unit": "V",
                "Options": plain_options,
            },
        },
    )
    info = node.a.b.node_info
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
@pytest.mark.asyncio()
async def test_node_info_emergent_attributes(attribute, expected):
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
    info = node.a.b.node_info
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
@pytest.mark.asyncio()
async def test_node_info_emergent_attributes_case2(attribute, expected):
    node = await get_unittest_mocked_node(
        {
            "/a/b": {
                "Node": "/a/b",
                "Description": "abcde",
                "Properties": "",
                "Type": "ZIVectorData",
                "Unit": "V",
            },
        },
    )
    info = node.a.b.node_info
    assert getattr(info, attribute) == expected
