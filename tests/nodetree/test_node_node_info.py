from unittest.mock import patch

import pytest
from labone.nodetree.node import (
    NodeInfo,
)


class TestNodeInfo:
    def test_getattr(self):
        info = NodeInfo({"Properties": "p"})
        assert info.Properties == "p"
        assert info.properties == "p"

    def test_contains(self):
        with patch.object(
            NodeInfo,
            "__dir__",
            autospec=True,
            return_value=["a"],
        ) as dir_mock:
            info = NodeInfo({})
            assert "a" in info
            dir_mock.assert_called_once_with(info)

    @pytest.mark.parametrize(
        ("other_path", "expected"),
        [
            ("/a", True),
            ("/b", False),
        ],
    )
    def test_hash(self, other_path, expected):
        assert (
            hash(NodeInfo({"Node": "/a"})) == hash(NodeInfo({"Node": other_path}))
        ) == expected

    @pytest.mark.parametrize(
        ("other_info", "expected"),
        [
            ({"Node": "/a"}, True),
            ({"Node": "/a", "Description": ""}, False),
        ],
    )
    def test_eq(self, other_info, expected):
        assert (NodeInfo({"Node": "/a"}) == NodeInfo(other_info)) == expected

    def test_eq_other_type(self):
        assert NodeInfo({"Node": "/a"}) != "/a"

    def test_dir(self):
        info = NodeInfo(
            {
                "Description": "d",
                "Type": "Double",
                "Unit": "u",
                "Node": "/a/b/c",
                "Properties": "p",
                "Options": {"1": "a", "2": "b"},
            },
        )
        assert {
            "description",
            "is_setting",
            "is_vector",
            "path",
            "readable",
            "type",
            "unit",
            "writable",
            "options",
        } <= set(dir(info))

    def test_repr(self):
        info = NodeInfo({"a": 1, "Node": "/a/b/c"})
        assert "/a/b/c" in repr(info)

    def test_str(self, zi_structure):
        info = NodeInfo(zi_structure.nodes_to_info["/zi/mds/groups/0/status"])
        assert (
            str(info)
            == "/zi/mds/groups/0/status\nIndicates the status the synchronization "
            "group.\nProperties: Read, Write, Setting\nType: Integer (enumerated)"
            "\nUnit: None\nOptions:\n    -1: Error. An error occurred in the "
            "synchronization process.\n    0: New\n    1: Sync\n    2: Alive"
        )

    @pytest.mark.parametrize(
        ("info", "expected_write", "expected_read", "expected_setting"),
        [
            ("Read, Write, Setting", True, True, True),
            ("Write, Setting", True, False, True),
            ("Read", False, True, False),
            ("", False, False, False),
        ],
    )
    def test_properties(self, info, expected_write, expected_read, expected_setting):
        info = NodeInfo({"Properties": info})
        assert info.writable == expected_write
        assert info.readable == expected_read
        assert info.is_setting == expected_setting

    @pytest.mark.parametrize(
        ("info", "expected"),
        [
            ("Vector", True),
            ("", False),
        ],
    )
    def test_is_vector(self, info, expected):
        info = NodeInfo({"Type": info})
        assert info.is_vector == expected

    def test_path(self):
        info = NodeInfo({"Node": "/a/B/c"})
        assert info.path == "/a/b/c"

    def test_options(self):
        info = NodeInfo(
            {
                "Options": {
                    "-1": "Error. An error occurred in the synchronization process.",
                    "0": "New",
                    "1": "Sync",
                    "2": '"Alive": Device is reachable.',
                },
            },
        )
        expected = {
            -1: "Error. An error occurred in the synchronization process.",
            0: "New",
            1: "Sync",
            2: "Device is reachable.",
        }
        expected_enum = {-1: "", 0: "", 1: "", 2: "Alive"}
        for k, v in info.options.items():
            assert v.description == expected[k]
            assert v.enum == expected_enum[k]

    def test_description(self):
        info = NodeInfo({"Description": "test description"})
        assert info.description == "test description"

    def test_type(self):
        info = NodeInfo({"Type": "test type"})
        assert info.type == "test type"

    def test_unit(self):
        info = NodeInfo({"Unit": "test unit"})
        assert info.unit == "test unit"
