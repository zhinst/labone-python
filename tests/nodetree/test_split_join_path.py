import pytest

from labone.nodetree.helper import join_path, split_path


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
