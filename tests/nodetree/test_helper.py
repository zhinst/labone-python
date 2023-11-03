import pytest
from labone.nodetree.helper import (
    UndefinedStructure,
    _build_nested_dict_recursively,
    build_prefix_dict,
    get_prefix,
    join_path,
    nested_dict_access,
    normalize_path_segment,
    pythonify_path_segment,
    split_path,
)

from tests.nodetree.conftest import zi_structure


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
        assert zi_structure.structure == nested_dict_access((), zi_structure.structure)

    def test_normal_access(self):
        assert zi_structure.structure["zi"]["config"] == nested_dict_access(
            ("zi", "config"),
            zi_structure.structure,
        )

    def test_long_access(self):
        assert zi_structure.structure["zi"]["mds"]["groups"]["0"][
            "devices"
        ] == nested_dict_access(
            ("zi", "mds", "groups", "0", "devices"),
            zi_structure.structure,
        )

    def test_wrong_access(self):
        with pytest.raises(KeyError):
            nested_dict_access(("wrong", "access"), zi_structure.structure)

    def test_too_long_access(self):
        with pytest.raises(KeyError):
            nested_dict_access(
                ("zi", "config", "port", "too_long"),
                zi_structure.structure,
            )


class TestJoinSplitPath:
    @staticmethod
    def test_join_path():
        assert join_path(["a", "b", "c"]) == "/a/b/c"
        assert join_path([]) == "/"

    @staticmethod
    def test_split_path():
        assert split_path("/a/b/c") == ["a", "b", "c"]
        assert split_path("/") == []
        assert split_path("a/b/c") == ["a", "b", "c"]

    @staticmethod
    def test_annihilation_split_join():
        path = "/a/b/c"
        assert join_path(split_path(path)) == path

        path = "/"
        assert join_path(split_path(path)) == path

        path = "/c"
        assert join_path(split_path(path)) == path

    @staticmethod
    @pytest.mark.parametrize(
        ("path_snippet1", "path_snippet2"),
        [
            ("/a/b/c", "/d/e/f"),
            ("/a/b", "/c/d/e/f"),
            ("/", "a/b/c"),
        ],
    )
    def test_split_associativity(path_snippet1, path_snippet2):
        assert (
            join_path(split_path(path_snippet1) + split_path(path_snippet2))
            == path_snippet1 + path_snippet2
        )


def test_get_prefix():
    assert get_prefix("/a/b/c", 2) == "/a/b"
    assert get_prefix("/a/b/c", 0) == "/"
    assert get_prefix("/a/b/c", 3) == "/a/b/c"
    assert get_prefix("/a/b/c", 4) == "/a/b/c"
    assert get_prefix("/", 1) == "/"
    assert get_prefix("/", 0) == "/"


class TestNormalizePythonifyPath:
    @staticmethod
    def test_normalize_path_segment():
        assert normalize_path_segment("a") == "a"
        assert normalize_path_segment("A") == "a"
        assert normalize_path_segment("a_") == "a"
        assert normalize_path_segment("A_") == "a"
        assert normalize_path_segment("a_1") == "a_1"
        assert normalize_path_segment("A_1") == "a_1"
        assert normalize_path_segment(1) == "1"
        assert normalize_path_segment("1.0") == "1.0"

    @staticmethod
    def test_pythonify_path_segment():
        assert pythonify_path_segment("a") == "a"
        assert pythonify_path_segment("in") == "in_"
        assert pythonify_path_segment("1.0") == "1.0"
        assert isinstance(pythonify_path_segment("1"), str)


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
def test_build_prefix_dict(input_value, expected_output):
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
