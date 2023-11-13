from unittest.mock import patch

import pytest
from labone.nodetree.helper import (
    UndefinedStructure,
    _build_nested_dict_recursively,
    build_prefix_dict,
    get_prefix,
    join_path,
    nested_dict_access,
    normalize_path_segment,
    paths_to_nested_dict,
    pythonify_path_segment,
    split_path,
)


class TestUndefinedStructure:
    def test_getitem(self):
        structure = UndefinedStructure()

        for extension in ["do", "anything", "you", "want"]:
            assert isinstance(structure[extension], UndefinedStructure)

    def test_eq(self):
        assert UndefinedStructure() == UndefinedStructure()
        assert (
            UndefinedStructure()
            == UndefinedStructure()["do"]["anything"]["you"]["want"]
        )


class TestNestedDictAccess:
    def test_empty_access(self):
        assert {} == nested_dict_access((), {})

    def test_normal_access(self, zi_structure):
        assert zi_structure.structure["zi"]["config"] == nested_dict_access(
            ("zi", "config"),
            zi_structure.structure,
        )

    def test_long_access(self, zi_structure):
        assert zi_structure.structure["zi"]["mds"]["groups"]["0"][
            "devices"
        ] == nested_dict_access(
            ("zi", "mds", "groups", "0", "devices"),
            zi_structure.structure,
        )

    def test_wrong_access(self, zi_structure):
        with pytest.raises(KeyError):
            nested_dict_access(("wrong", "access"), zi_structure.structure)

    def test_too_long_access(self, zi_structure):
        with pytest.raises(KeyError):
            nested_dict_access(
                ("zi", "config", "port", "too_long"),
                zi_structure.structure,
            )


class TestJoinSplitPath:
    @pytest.mark.parametrize(
        ("path_segments", "path"),
        [
            ([], "/"),
            (["a"], "/a"),
            (["a", "b", "c"], "/a/b/c"),
            (["a", "aa", "a", "aaa"], "/a/aa/a/aaa"),
        ],
    )
    def test_join_path(self, path_segments, path):
        assert join_path(path_segments) == path

    @pytest.mark.parametrize(
        ("path_segments", "path"),
        [
            ([], "/"),
            (["a"], "/a"),
            (["a", "b", "c"], "/a/b/c"),
            (["a", "aa", "a", "aaa"], "/a/aa/a/aaa"),
        ],
    )
    def test_split_path(self, path_segments, path):
        assert split_path(path) == path_segments

    @pytest.mark.parametrize(
        ("path_segments", "path"),
        [
            ([], "/"),
            (["a"], "/a"),
            (["a", "b", "c"], "/a/b/c"),
            (["a", "aa", "a", "aaa"], "/a/aa/a/aaa"),
        ],
    )
    def test_annihilation_split_join(self, path_segments, path):
        assert join_path(split_path(path)) == path
        assert split_path(join_path(path_segments)) == path_segments

    @pytest.mark.parametrize(
        ("path_snippet1", "path_snippet2"),
        [
            ("/a/b/c", "/d/e/f"),
            ("/a/b", "/c/d/e/f"),
            ("/", "a/b/c"),
        ],
    )
    def test_split_associativity(self, path_snippet1, path_snippet2):
        assert (
            join_path(split_path(path_snippet1) + split_path(path_snippet2))
            == path_snippet1 + path_snippet2
        )


@pytest.mark.parametrize(
    ("path", "length", "prefix"),
    [
        ("/a/b/c", 2, "/a/b"),
        ("/a/b/c", 0, "/"),
        ("/a/b/c", 3, "/a/b/c"),
        ("/a/b/c", 4, "/a/b/c"),
        ("/", 1, "/"),
        ("/", 0, "/"),
    ],
)
def test_get_prefix(path, length, prefix):
    assert get_prefix(path, length) == prefix


@pytest.mark.parametrize(
    ("path_segment", "normalized_path_segment"),
    [
        ("a", "a"),
        ("A", "a"),
        ("a_", "a"),
        ("A_", "a"),
        ("a_1", "a_1"),
        ("A_1", "a_1"),
        (1, "1"),
        ("1.0", "1.0"),
    ],
)
def test_normalize_path_segment(path_segment, normalized_path_segment):
    assert normalize_path_segment(path_segment) == normalized_path_segment


@pytest.mark.parametrize(
    ("path_segment", "normalized_path_segment"),
    [
        ("a", "a"),
        ("in", "in_"),
        ("try", "try_"),
        ("1", "1"),
    ],
)
def test_pythonify_path_segment(path_segment, normalized_path_segment):
    assert pythonify_path_segment(path_segment) == normalized_path_segment


@pytest.mark.parametrize(
    "paths",
    [
        [],
        ["p1", "p2", "p3"],
    ],
)
def test_paths_to_nested_dict(paths):
    with patch(
        "labone.nodetree.helper.split_path",
        side_effect=lambda x: x,
        autospec=True,
    ) as split_mock, patch(
        "labone.nodetree.helper._build_nested_dict_recursively",
        return_value="result",
        autospec=True,
    ) as build_nested_mock:
        assert paths_to_nested_dict(paths) == "result"
        assert split_mock.call_count == len(paths)
        for p in paths:
            split_mock.assert_any_call(p)
        build_nested_mock.assert_called_once_with(paths)


@pytest.mark.parametrize(
    ("build_prefix_return", "sub_calls"),
    [
        ({}, []),
        ({"a": []}, [[]]),
        ({"a": [["b", "c"]]}, [[["b", "c"]]]),
        ({"a": [["b", "c"]], "d": [["e", "f"]]}, [[["b", "c"]], [["e", "f"]]]),
    ],
)
def test_build_nested_dict_recursively(build_prefix_return, sub_calls):
    with patch(
        "labone.nodetree.helper.build_prefix_dict",
        return_value=build_prefix_return,
        autospec=True,
    ) as build_prefix_mock, patch(
        "labone.nodetree.helper._build_nested_dict_recursively",
        autospec=True,
    ) as mock:
        _build_nested_dict_recursively("input")  # will be ignored in this test
        build_prefix_mock.assert_called_once_with("input")

        for s in sub_calls:
            mock.assert_any_call(s)


@pytest.mark.parametrize(
    ("input_value", "expected_output"),
    [
        ([], {}),
        ([["port"]], {"port": []}),
        ([["zi", "config", "open"]], {"zi": [["config", "open"]]}),
        (
            [["config", "port"], ["config", "open"]],
            {"config": [["port"], ["open"]]},
        ),
        (
            [["port"], ["open"], ["authors"]],
            {"port": [], "open": [], "authors": []},
        ),
        (
            [["config", "port"], ["config", "open"], ["other", "open"]],
            {"config": [["port"], ["open"]], "other": [["open"]]},
        ),
    ],
)
def test_build_prefix_dict_function_chain(input_value, expected_output):
    assert build_prefix_dict(input_value) == expected_output


@pytest.mark.parametrize(
    ("input_value", "expected_output"),
    [
        ([], {}),
        ([["port"]], {"port": {}}),
        ([["zi", "config", "open"]], {"zi": {"config": {"open": {}}}}),
        (
            [["config", "port"], ["config", "open"]],
            {"config": {"port": {}, "open": {}}},
        ),
    ],
)
def test_build_structure(input_value, expected_output):
    assert _build_nested_dict_recursively(input_value) == expected_output
