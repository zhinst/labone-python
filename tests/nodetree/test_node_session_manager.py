from unittest.mock import MagicMock, patch

import pytest
from labone.nodetree.errors import (
    LabOneInvalidPathError,
)
from labone.nodetree.helper import (
    WILDCARD,
)
from labone.nodetree.node import (
    Node,
    NodeTreeManager,
)

from tests.nodetree.conftest import (
    MockNodeTreeManager,
)


class TestNodeTreeManager:
    def test_init(self):
        path_to_info = {"/a/b/c": "info1"}

        with patch("labone.nodetree.node.build_prefix_dict") as prefix_mock:
            NodeTreeManager(
                session="session",
                parser="parser",
                path_to_info=path_to_info,
            )
            prefix_mock.assert_called_once_with([["a", "b", "c"]])

    @pytest.mark.parametrize(
        ("explored_structure", "hide_kernel_prefix", "expected_prefix"),
        [
            ({"a": [], "b": []}, False, ()),
            ({"a": ["/b/c", "/e"], "x": []}, True, ()),
            ({"a": ["/b/c", "/e"]}, True, ("a",)),
            ({"a": ["/b/c", "/e"]}, False, ()),
        ],
    )
    def test_construct_nodetree(
        self,
        explored_structure,
        hide_kernel_prefix,
        expected_prefix,
    ):
        manager = MockNodeTreeManager()
        manager._partially_explored_structure = explored_structure

        with patch.object(
            NodeTreeManager,
            "path_segments_to_node",
            autospec=True,
            return_value="node",
        ) as patch_path_to_node:
            result = manager.construct_nodetree(hide_kernel_prefix=hide_kernel_prefix)
            patch_path_to_node.assert_called_once_with(manager, expected_prefix)
            assert result == "node"

    @pytest.mark.parametrize(
        ("paths", "access_chain", "expected_structure"),
        [
            ([], (), {}),
            (["/a/b/c"], (), {"a": ["/b/c"]}),
            (["/a/b/c"], ("a",), {"b": ["/c"]}),
            (["/a/b/c"], ("a", "b", "c"), {}),
            (["/a/b/c", "/a/b/d"], ("a", "b"), {"c": [], "d": []}),
        ],
    )
    def test_find_substructure(self, paths, access_chain, expected_structure):
        manager = MockNodeTreeManager({p: None for p in paths})
        structure = manager.find_substructure(access_chain)
        assert structure.keys() == expected_structure.keys()

        # check indirectly via cache that recursive calls occured
        subchain = []
        for e in access_chain:
            subchain.append(e)
            assert (hash(manager), tuple(subchain)) in manager._cache_find_substructure

    def test_find_substructure_prebuild_structure(self):
        manager = MockNodeTreeManager()
        manager._partially_explored_structure = {"a": {"b": {"c": {}}}}
        assert manager.find_substructure(("a", "b")) == {"c": {}}

    def test_find_substructure_cached(self):
        manager = MockNodeTreeManager({"/a/b/c": None})
        manager._cache_find_substructure = {(hash(manager), ("a", "b", "c")): "cached"}
        assert manager.find_substructure(("a", "b", "c")) == "cached"

    @pytest.mark.parametrize(
        ("paths", "access_chain"),
        [
            ([], ("a")),
            (["/a/b/c"], (WILDCARD)),
            (["/a/b/c"], ("a", "c")),
            (["/a/b/c"], ("a", "b", "c", "d")),
        ],
    )
    def test_find_structure_raises(self, paths, access_chain):
        manager = MockNodeTreeManager({p: None for p in paths})
        with pytest.raises(LabOneInvalidPathError):
            manager.find_substructure(access_chain)

    def test_raw_path_to_node(self):
        with patch.object(
            NodeTreeManager,
            "path_segments_to_node",
            autospec=True,
        ) as patch_path_to_node, patch(
            "labone.nodetree.node.split_path",
            autospec=True,
            return_value=["path", "segments"],
        ) as patch_split:
            manager = MockNodeTreeManager()
            manager.raw_path_to_node("path")
            patch_split.assert_called_once_with("path")
            patch_path_to_node.assert_called_once_with(manager, ("path", "segments"))

    def test_path_segments_to_node(self):
        manager = MockNodeTreeManager()
        with patch.object(Node, "build", autospec=True) as build_patch:
            manager.path_segments_to_node(("path", "segments"))
            build_patch.assert_called_once_with(
                tree_manager=manager,
                path_segments=("path", "segments"),
            )

            # check cached behavior
            build_patch.reset_mock()
            manager.path_segments_to_node(("path", "segments"))
            build_patch.assert_not_called()

    def test_hash(self):
        manager = MockNodeTreeManager()
        assert hash(manager) == id(manager)

    def test_paths(self):
        paths = {"/a/b/c": None, "/a/b/d": None}
        manager = MockNodeTreeManager(paths)
        assert manager.paths == paths.keys()

    def test_parser(self):
        parser = MagicMock()
        manager = NodeTreeManager(
            session=None,
            path_to_info={},
            parser=parser,
        )
        assert manager.parser == parser

    def test_session(self):
        session = MagicMock()
        manager = NodeTreeManager(
            session=session,
            path_to_info={},
            parser=None,
        )
        assert manager.session == session
