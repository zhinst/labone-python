from unittest.mock import MagicMock, Mock, create_autospec, patch

import pytest
from labone.core import AnnotatedValue
from labone.core.subscription import DataQueue
from labone.nodetree.errors import (
    LabOneInappropriateNodeTypeError,
    LabOneInvalidPathError,
    LabOneNotImplementedError,
)
from labone.nodetree.helper import (
    WILDCARD,
    Session,
    UndefinedStructure,
    join_path,
    split_path,
)
from labone.nodetree.node import (
    LeafNode,
    MetaNode,
    Node,
    NodeInfo,
    NodeTreeManager,
    PartialNode,
    ResultNode,
    WildcardNode,
)

from tests.nodetree.conftest import (
    MockLeafNode,
    MockMetaNode,
    MockNode,
    MockPartialNode,
    MockResultNode,
    MockWildcardNode,
    MockWildcardOrPartialNode,
    _get_future,
)


class TestMetaNode:
    @pytest.mark.parametrize(
        ("subtree_structure"),
        [
            ({"a": [], "b": [], "c": []}),
            {},
            {"a": []},
            {"b": [], "a": []},
        ],
    )
    def test_iter(self, subtree_structure, result_node):
        result_node._subtree_paths = subtree_structure

        sub_list = sorted(subtree_structure.keys())

        with patch(
            "labone.nodetree.node.ResultNode.__getitem__",
            side_effect=[0, 1, 2],
            autospec=True,
        ) as patch_getitem:
            for i, subnode in enumerate(result_node):
                patch_getitem.assert_called_with(result_node, sub_list[i])
                assert subnode == i

    @pytest.mark.parametrize(
        ("subtree_structure"),
        [({"a": [], "b": [], "c": []}), {}, {"a": []}, {"b": [], "a": []}],
    )
    def test_len(self, subtree_structure, result_node):
        result_node._subtree_paths = subtree_structure

        assert len(result_node) == len(subtree_structure)

    @pytest.mark.parametrize(
        ("path_aliases", "start_path", "expected"),
        [
            ({"a": "b"}, "a", "b"),
            ({}, "a", "a"),
            ({"a": "b", "b": "c"}, "a", "c"),
            ({"a": "b", "b": "c"}, "b", "c"),
            ({"a": "b", "b": "c"}, "x", "x"),
            ({"a/b": "c/d", "c/d": "e/f"}, "a/b", "e/f"),
        ],
    )
    def test_redirect(self, path_aliases, start_path, expected, zi):
        zi._path_aliases = path_aliases
        assert zi._redirect(start_path) == expected

    @pytest.mark.parametrize(
        (
            "start_path_segments",
            "subtree_structure",
            "redirect_dict",
            "test_path_segments",
            "expected",
        ),
        [
            ((), [], {}, (), False),
            (("a",), [], {}, ("a",), False),
            ((), ["a"], {}, ("a",), True),
            (("a", "b"), [], {}, ("a", "b"), False),
            (("a",), ["c"], {}, ("a", "c"), True),
            (("a", "b"), ["c"], {}, ("a", "b", "c"), True),
            (("a", "b"), ["c"], {}, ("a", "b", "d"), False),
            (("a", "b", "c", "d"), ["d"], {}, ("a", "b", "c", "d", "d"), True),
            (("a",), ["c"], {}, ("a", "c", "d"), False),
            (("a",), ["c"], {}, ("a", "c", "d", "e", "f", "g"), False),
            (
                ("x", "y"),
                ["c"],
                {("x", "y", "c"): ("a", "b", "c", "d", "e")},
                ("a", "b", "c", "d", "e"),
                True,
            ),
        ],
    )
    @pytest.mark.parametrize("as_node", [True, False])
    def test_is_child_node(  # noqa: PLR0913
        self,
        as_node,
        start_path_segments,
        subtree_structure,
        redirect_dict,
        test_path_segments,
        expected,
    ):
        node = MockMetaNode(start_path_segments)
        node._subtree_paths = {s: None for s in subtree_structure}

        def new_redirect(_, path):
            if path in redirect_dict:
                return redirect_dict[path]
            return path

        with patch.object(
            MetaNode,
            "_redirect",
            autospec=True,
            side_effect=new_redirect,
        ) as redirect_mock:
            sub = MockMetaNode(test_path_segments) if as_node else test_path_segments
            assert node.is_child_node(sub) == expected
            for s in subtree_structure:
                redirect_mock.assert_any_call(node, (*start_path_segments, s))

    def test_path(self):
        with patch("labone.nodetree.node.join_path", return_value="/") as join_mock:
            node = MockMetaNode(())
            assert node.path == "/"
            join_mock.assert_called_once_with(())

    def test_str(self):
        node = MockMetaNode(("a", "b"))
        assert node.path in str(node)

    def test_repr(self):
        node = MockMetaNode(("a", "b"))
        assert node.path in repr(node)

    def test_raw_tree(self):
        with pytest.warns(DeprecationWarning):
            assert MockMetaNode(()).raw_tree == ()


class TestResultNode:
    def test_getattr(self, result_node):
        with patch(
            "labone.nodetree.node.normalize_path_segment",
            side_effect=lambda x: x,
            autospec=True,
        ) as patch_normalize, patch.object(
            ResultNode,
            "try_generate_subnode",
            return_value="subnode",
            autospec=True,
        ):
            subnode = result_node.__getattr__("next")

            patch_normalize.assert_called_once_with("next")
            ResultNode.try_generate_subnode.assert_called_once_with(result_node, "next")
            assert subnode == "subnode"

    @pytest.mark.parametrize(
        "path_segments",
        [
            (),
            ("next",),
            ("next", "next2", "next3"),
        ],
    )
    def test_getitem(self, path_segments):
        path = join_path(path_segments)

        with patch(
            "labone.nodetree.node.split_path",
            side_effect=lambda x: split_path(x),
            autospec=True,
        ) as split_patch, patch(
            "labone.nodetree.node.normalize_path_segment",
            side_effect=lambda x: x,
            autospec=True,
        ) as normalize_patch, patch.object(
            ResultNode,
            "try_generate_subnode",
            Mock(
                side_effect=[
                    MockResultNode(path_segments[: i + 1])
                    for i in range(len(path_segments))
                ],
            ),
        ) as try_generate_patch, patch.object(
            ResultNode,
            "__repr__",
            return_value="some_node",
            autospec=True,
        ):
            node = MockResultNode(())
            subnode = node.__getitem__(path)

            assert subnode.path == path
            split_patch.assert_called_once_with(path)

            for i in range(len(path_segments)):
                normalize_patch.assert_any_call(path_segments[i])
                try_generate_patch.assert_any_call(path_segments[i])

    def test_getitem_too_long(self):
        segments = ("next", "next2")

        with patch(
            "labone.nodetree.node.normalize_path_segment",
            side_effect=lambda x: x,
            autospec=True,
        ) as normalize_patch, patch(
            "labone.nodetree.node.ResultNode.try_generate_subnode",
            side_effect=[AnnotatedValue(path="next", value=1)],
            autospec=True,
        ) as try_generate_patch:
            node = MockResultNode(())
            with pytest.raises(LabOneInvalidPathError):
                node.__getitem__(join_path(segments))

            normalize_patch.assert_called_once_with("next")
            try_generate_patch.assert_called_once_with(node, "next")

    @pytest.mark.parametrize(
        "keys",
        [
            {"a"},
            {"a", "b", "c", "d"},
        ],
    )
    def test_dir(self, keys):
        result_node = MockResultNode(())
        result_node._subtree_paths = {k: [] for k in keys}

        with patch(
            "labone.nodetree.node.pythonify_path_segment",
            side_effect=lambda x: x,
            autospec=True,
        ) as pythonify_mock:
            assert keys <= set(dir(result_node))
            for k in keys:
                pythonify_mock.assert_any_call(k)

    @pytest.mark.parametrize(
        ("next_segment", "subtree_paths", "expected"),
        [
            ("a", set(), False),
            ("a", {"a", "b"}, True),
            ("0", {"0"}, True),
            ("c", {"a", "c", "e", "f"}, True),
            ("d", {"a", "c", "e", "f"}, False),
        ],
    )
    def test_contains_plain_value(self, next_segment, subtree_paths, expected):
        node = MockResultNode(())
        node._subtree_paths = subtree_paths

        with patch(
            "labone.nodetree.node.normalize_path_segment",
            side_effect=lambda x: x,
            autospec=True,
        ) as normal_patch:
            decision = next_segment in node
            normal_patch.assert_called_once_with(next_segment)
            assert decision == expected

    @pytest.mark.parametrize(
        ("next_segment"),
        [
            "a",
            "b",
        ],
    )
    def test_contains_node(self, next_segment):
        arg = MockResultNode((next_segment,))

        with patch(
            "labone.nodetree.node.ResultNode.is_child_node",
            autospec=True,
        ) as child_patch:
            node = MockResultNode(())
            arg in node  # noqa: B015
            child_patch.assert_called_once_with(node, arg)

    @pytest.mark.parametrize(
        ("next_segment"),
        [
            "a",
            "b",
        ],
    )
    def test_contains_annotated_value(self, next_segment):
        path = join_path(next_segment)

        with patch(
            "labone.nodetree.node.ResultNode.is_child_node",
            autospec=True,
        ) as child_patch, patch(
            "labone.nodetree.node.split_path",
            return_value=next_segment,
            autospec=True,
        ) as split_patch:
            node = MockResultNode(())
            AnnotatedValue(path=path, value=0) in node  # noqa: B015
            split_patch.assert_called_once_with(path)
            child_patch.assert_called_once_with(node, next_segment)

    def test_call_raises(self):
        with pytest.raises(LabOneInappropriateNodeTypeError):
            MockResultNode(()).__call__()

    def test_str(self, result_node):
        assert result_node.path in str(result_node)

    def test_repr(self, result_node):
        assert result_node.path in repr(result_node)

    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            ("/a", {1, 2, 3, 4, 5}),
            ("/b", set()),
            ("/a/b", {1}),
            ("/a/c", {2, 3, 4, 5}),
            ("/a/c/e", {3}),
            ("/a/c/e/e", set()),
            ("/a/c/f", {4, 5}),
        ],
    )
    def test_results(self, path, expected):
        value_structure = {
            "/a/b": 1,
            "/a/c/d": 2,
            "/a/c/e": 3,
            "/a/c/f/g": 4,
            "/a/c/f/h": 5,
        }
        node = MockResultNode(tuple(split_path(path)))
        node._value_structure = value_structure

        assert set(node.results()) == expected

    def test_try_generate_subnode(self, result_node):
        segments = ("zi", "next")
        with patch(
            "labone.nodetree.node.MetaNode._redirect",
            side_effect=lambda self, x: x,  # noqa: ARG005
            autospec=True,
        ) as patch_redirect, patch(
            "labone.nodetree.node.ResultNode.__init__",
            return_value=None,
        ) as patch_init, patch.object(
            NodeTreeManager,
            "find_substructure",
            return_value={"level": []},
            autospec=True,
        ) as patch_find_structure:
            deeper = result_node.try_generate_subnode(next_path_segment="next")

            patch_redirect.assert_called_once_with(result_node, segments)
            patch_find_structure.assert_called_once_with(
                result_node.tree_manager,
                segments,
            )
            patch_init.assert_called_once_with(
                tree_manager=result_node.tree_manager,
                path_segments=(*result_node.path_segments, "next"),
                subtree_paths={"level": []},
                value_structure=result_node._value_structure,
                timestamp=result_node._timestamp,
                path_aliases=result_node.path_aliases,
            )
            assert isinstance(deeper, ResultNode)

    def test_try_generate_subnode_leaf(self, result_node):
        segments = ("zi", "next")
        path = "/zi/next"
        value = AnnotatedValue(path=path, value=1)

        def fake_init_func(self, *_, **__):
            self._path_segments = segments

        with patch(
            "labone.nodetree.node.MetaNode._redirect",
            return_value=segments,
            autospec=True,
        ) as patch_redirect, patch.object(
            NodeTreeManager,
            "find_substructure",
            return_value={},
            autospec=True,
        ) as patch_find_structure, patch.dict(
            result_node._value_structure,
            {path: value},
        ), patch.object(
            ResultNode,
            "__init__",
            fake_init_func,
        ):
            deeper = result_node.try_generate_subnode(next_path_segment="next")

            patch_redirect.assert_called_once_with(result_node, segments)
            patch_find_structure.assert_called_once_with(
                result_node.tree_manager,
                segments,
            )

            assert deeper == value

    def test_try_generate_subnode_invalid(self, result_node):
        segments = ("zi", "next")
        with patch(
            "labone.nodetree.node.MetaNode._redirect",
            side_effect=lambda self, x: x,  # noqa: ARG005
            autospec=True,
        ) as patch_redirect, patch.object(
            NodeTreeManager,
            "find_substructure",
            side_effect=LabOneInvalidPathError(),
            autospec=True,
        ) as patch_find_structure:
            with pytest.raises(LabOneInvalidPathError):
                result_node.try_generate_subnode(next_path_segment="next")

            patch_redirect.assert_called_once_with(result_node, segments)
            patch_find_structure.assert_called_once_with(
                result_node.tree_manager,
                segments,
            )

    def test_try_generate_subnode_wildcard(self, result_node):
        segments = ("zi", WILDCARD)
        with patch(
            "labone.nodetree.node.MetaNode._redirect",
            side_effect=lambda self, x: x,  # noqa: ARG005
            autospec=True,
        ) as patch_redirect, patch.object(
            NodeTreeManager,
            "find_substructure",
            side_effect=LabOneInvalidPathError(),
            autospec=True,
        ) as patch_find_structure:
            with pytest.raises(LabOneInvalidPathError):
                result_node.try_generate_subnode(next_path_segment=WILDCARD)

            patch_redirect.assert_called_once_with(result_node, segments)
            patch_find_structure.assert_called_once_with(
                result_node.tree_manager,
                segments,
            )


class TestNode:
    """
    Note: Some tests of this class are done via mock nodes of subclasses.
        These methods are not overritten, so the result stays the same.
        Reason: the MockNode class seems to interact very strangely
        with the pytest framework.
    """

    def test_getattr(self, zi):
        with patch(
            "labone.nodetree.node.normalize_path_segment",
            side_effect=lambda x: x,
            autospec=True,
        ) as patch_normalize, patch(
            "labone.nodetree.node.PartialNode.try_generate_subnode",
            side_effect=["subnode"],
            autospec=True,
        ) as patch_try_generate:
            subnode = zi.__getattr__("next")

            patch_normalize.assert_called_once_with("next")
            patch_try_generate.assert_called_once_with(zi, "next")
            assert subnode == "subnode"

    @pytest.mark.parametrize(
        "path_segments",
        [
            (),
            ("next",),
            ("next", "next2", "next3"),
        ],
    )
    def test_getitem(self, path_segments):
        path = join_path(path_segments)

        with patch(
            "labone.nodetree.node.split_path",
            side_effect=lambda x: split_path(x),
            autospec=True,
        ) as split_patch, patch(
            "labone.nodetree.node.normalize_path_segment",
            side_effect=lambda x: x,
            autospec=True,
        ) as normalize_patch, patch(
            "labone.nodetree.node.PartialNode.try_generate_subnode",
            side_effect=[
                MockPartialNode(path_segments[: i + 1])
                for i in range(len(path_segments))
                # adding autospec=True would break test
            ],
        ) as try_generate_patch:
            node = MockPartialNode(())
            subnode = node.__getitem__(path)

            assert subnode.path == path
            split_patch.assert_called_once_with(path)

            for i in range(len(path_segments)):
                normalize_patch.assert_any_call(path_segments[i])
                try_generate_patch.assert_any_call(path_segments[i])

    @pytest.mark.parametrize(
        ("node", "obj", "manager_equal", "expected"),
        [
            (MockNode(()), MockNode(()), True, True),
            (MockNode(()), MockNode(()), False, False),
            (MockNode(()), MockNode(("a",)), True, False),
            (MockNode(()), MockNode(("a",)), False, False),
            (MockNode(("a", "b")), MockNode(("a", "b")), True, True),
            (MockNode(("a", "b")), MockNode(("a", "b")), False, False),
            (MockNode(("a", "b")), MockResultNode(("a", "b")), True, False),
        ],
    )
    def test_eq(self, node, obj, manager_equal, expected):
        # simulate different managers. NodeManager has no __eq__ method, so
        # the object method is used.
        obj._tree_manager = None if manager_equal else 42
        assert (node == obj) == expected

    @pytest.mark.parametrize(
        ("node", "obj", "expected"),
        [
            (MockNode(("a", "b")), 1, False),
            (MockNode(("a", "b")), ("a", "b"), False),
        ],
    )
    def test_eq_other_class(self, node, obj, expected):
        assert (node == obj) == expected

    # do not forget to adjust these numbers if you change the number of different nodes
    @pytest.mark.parametrize("i", range(7))
    @pytest.mark.parametrize("j", range(7))
    def test_hash(self, i, j):
        manager_mock = create_autospec(NodeTreeManager)
        manager_mock.__hash__.return_value = 0
        manager_mock2 = MagicMock()
        manager_mock2.__hash__.return_value = 1

        with_other_manager = MockNode(())
        with_other_manager._tree_manager = manager_mock2

        should_be_mutually_diffrent = [
            MockNode(()),
            MockNode(("a", "b", "c")),
            MockNode(("a", "b", "c", "d")),
            MockLeafNode(()),
            MockWildcardNode(()),
            MockWildcardNode(("a",)),
        ]
        for i in range(len(should_be_mutually_diffrent)):
            should_be_mutually_diffrent[i]._tree_manager = manager_mock
        should_be_mutually_diffrent.append(with_other_manager)

        same_one = i == j
        one = should_be_mutually_diffrent[i]
        other = should_be_mutually_diffrent[j]

        assert (hash(one) != hash(other)) ^ same_one
        one.tree_manager.__hash__.assert_any_call()
        other.tree_manager.__hash__.assert_any_call()

    @pytest.mark.parametrize(
        "keys",
        [
            {"a"},
            {"a", "b", "c", "d"},
        ],
    )
    def test_dir(self, keys):
        node = MockPartialNode(())
        node._subtree_paths = {k: [] for k in keys}

        with patch(
            "labone.nodetree.node.pythonify_path_segment",
            side_effect=lambda x: x,
            autospec=True,
        ) as pythonify_mock:
            assert keys <= set(dir(node))
            for k in keys:
                pythonify_mock.assert_any_call(k)

    @pytest.mark.parametrize(
        ("next_segment"),
        [
            "a",
            "b",
        ],
    )
    def test_contains_node(self, next_segment):
        arg = MockPartialNode((next_segment,))

        with patch(
            "labone.nodetree.node.MetaNode.is_child_node",
            autospec=True,
        ) as child_patch:
            node = MockPartialNode(())
            arg in node  # noqa: B015
            child_patch.assert_called_once_with(node, arg)

    @pytest.mark.parametrize(
        ("next_segment", "subtree_paths", "expected"),
        [
            ("a", set(), False),
            ("a", {"a", "b"}, True),
            ("0", {"0"}, True),
            ("c", {"a", "c", "e", "f"}, True),
            ("d", {"a", "c", "e", "f"}, False),
        ],
    )
    def test_contains_plain_value(self, next_segment, subtree_paths, expected):
        node = MockPartialNode(())
        node._subtree_paths = subtree_paths

        with patch(
            "labone.nodetree.node.normalize_path_segment",
            side_effect=lambda x: x,
            autospec=True,
        ) as normal_patch:
            decision = next_segment in node
            normal_patch.assert_called_once_with(next_segment)
            assert decision == expected

    @pytest.mark.asyncio()
    async def test_call_get(self):
        with patch("labone.nodetree.node.PartialNode._get", autospec=True) as get_patch:
            node = MockPartialNode(())
            await node()
            get_patch.assert_called_once_with(node)

    @pytest.mark.asyncio()
    async def test_call_set(self):
        with patch("labone.nodetree.node.PartialNode._set", autospec=True) as set_patch:
            node = MockPartialNode(())
            await node("argument")
            set_patch.assert_called_once_with(node, "argument")

    @pytest.mark.parametrize(
        ("path_segments"),
        [(WILDCARD,), ("a", WILDCARD, "c"), (WILDCARD, WILDCARD, WILDCARD)],
    )
    def test_build_wildcard(self, path_segments):
        node = Node.build(
            tree_manager="tree_manager",
            path_segments=path_segments,
            path_aliases="path_aliases",
        )

        assert isinstance(node, WildcardNode)
        assert node._tree_manager == "tree_manager"
        assert node._path_segments == path_segments
        assert node._path_aliases == "path_aliases"
        assert isinstance(node.subtree_paths, UndefinedStructure)

    @pytest.mark.parametrize(
        "path_segments",
        [
            (),
            ("a",),
            ("a", "b", "c"),
        ],
    )
    def test_build_leaf(self, path_segments):
        tree_manager = create_autospec(NodeTreeManager)
        tree_manager.find_substructure.return_value = {}

        node = Node.build(
            tree_manager=tree_manager,
            path_segments=path_segments,
            path_aliases="path_aliases",
        )

        tree_manager.find_substructure.assert_called_once_with(path_segments)

        assert isinstance(node, LeafNode)
        assert node._tree_manager == tree_manager
        assert node._path_segments == path_segments
        assert node._path_aliases == "path_aliases"
        assert node.subtree_paths == {}

    @pytest.mark.parametrize(
        "path_segments",
        [
            (),
            ("a",),
            ("a", "b", "c"),
        ],
    )
    @pytest.mark.parametrize(
        "subtree_paths",
        [
            {"z": []},
            {"z": [], "x": []},
            {"z": ["c/d", "e/f"]},
        ],
    )
    def test_build_partial(self, path_segments, subtree_paths):
        tree_manager = create_autospec(NodeTreeManager)
        tree_manager.find_substructure.return_value = subtree_paths

        node = Node.build(
            tree_manager=tree_manager,
            path_segments=path_segments,
            path_aliases="path_aliases",
        )

        tree_manager.find_substructure.assert_called_once_with(path_segments)

        assert isinstance(node, PartialNode)
        assert node._tree_manager == tree_manager
        assert node._path_segments == path_segments
        assert node._path_aliases == "path_aliases"
        assert node.subtree_paths == subtree_paths

    @pytest.mark.parametrize(
        "path_segments",
        [
            (),
            ("a"),
            ("a", "b"),
            ("c", "v", "r"),
        ],
    )
    def test_root(self, path_segments):
        node = MockNode(path_segments=path_segments)
        node._tree_manager = create_autospec(NodeTreeManager)

        node.root  # noqa: B018

        node.tree_manager.path_segments_to_node.assert_called_once_with(())


class TestLeafNode:
    @pytest.mark.asyncio()
    async def test_get(self, mock_path):
        node = MockLeafNode(())
        node._tree_manager = create_autospec(NodeTreeManager)
        node._tree_manager._session = create_autospec(Session)
        node._tree_manager.parser.return_value = "parser_response"
        node._tree_manager.session.get.return_value = _get_future("get_response")

        result = await node._get()
        mock_path.assert_called_once()
        node.tree_manager.session.get.assert_called_once_with("path")
        node._tree_manager.parser.assert_called_once_with("get_response")
        assert result == "parser_response"

    @pytest.mark.asyncio()
    async def test_set(self, mock_path):
        node = MockLeafNode(())
        node._tree_manager = create_autospec(NodeTreeManager)
        node._tree_manager._session = create_autospec(Session)
        node._tree_manager.parser.return_value = "parser_response"
        node._tree_manager.session.set.return_value = _get_future("set_response")

        result = await node._set(value="value")
        mock_path.assert_called_once()
        node._tree_manager.session.set.assert_called_once_with(
            AnnotatedValue(path="path", value="value"),
        )
        node._tree_manager.parser.assert_called_once_with("set_response")
        assert result == "parser_response"

    def test_try_generate_subnode_raises(self):
        with pytest.raises(LabOneInvalidPathError):
            MockLeafNode(()).try_generate_subnode("next")

    @pytest.mark.asyncio()
    async def test_subscribe(self, mock_path):
        node = MockLeafNode(())
        node._tree_manager = create_autospec(NodeTreeManager)
        node._tree_manager._parser = create_autospec(lambda x: x)
        node._tree_manager.parser.return_value = "parser"
        node._tree_manager._session = create_autospec(Session)
        node._tree_manager.session.subscribe.return_value = _get_future(
            "subscribe_response",
        )

        await node.subscribe()

        mock_path.assert_called_once_with()
        node._tree_manager.session.subscribe.assert_called_once_with(
            "path",
            parser_callback=node._tree_manager.parser,
            queue_type=DataQueue,
        )

    @pytest.mark.parametrize(
        ("target_value", "current_value", "invert", "expect_early_termination"),
        [
            (1, 1, False, True),
            (1, 1, True, False),
            (1, 2, False, False),
            (2, 1, False, False),
            (1, 2, True, True),
        ],
    )
    @pytest.mark.asyncio()
    async def test_wait_for_state_change(
        self,
        target_value,
        current_value,
        invert,
        expect_early_termination,
    ):
        node = MockLeafNode(())
        node.subscribe = Mock(return_value=_get_future("queue"))
        future = _get_future(AnnotatedValue(path="path", value=current_value))

        with patch(
            "labone.nodetree.node.LeafNode._wait_for_state_change_loop",
            autospec=True,
        ) as loop_patch, patch(
            "labone.nodetree.node.Node.__call__",
            MagicMock(return_value=future),
        ) as call_patch:
            await node.wait_for_state_change(value=target_value, invert=invert)

            call_patch.assert_called_once_with()
            node.subscribe.assert_called_once_with()

            if expect_early_termination:
                loop_patch.assert_not_called()
            else:
                loop_patch.assert_called_once_with(
                    "queue",
                    value=target_value,
                    invert=invert,
                )

    @pytest.mark.parametrize(
        ("target_value", "in_pipe", "invert", "nr_expected_calls"),
        [
            (1, [1], False, 1),
            (1, [1, 2, 3], False, 1),
            (1, [4, 2, 1], False, 3),
            (2, [2, 2, 2, 4, 2], True, 4),
        ],
    )
    @pytest.mark.asyncio()
    async def test_wait_for_state_change_loop(
        self,
        target_value,
        in_pipe,
        invert,
        nr_expected_calls,
    ):
        node = MockLeafNode(())
        queue = create_autospec(DataQueue)
        queue.get.side_effect = [AnnotatedValue(value=i, path="/") for i in in_pipe]

        await node._wait_for_state_change_loop(queue, target_value, invert=invert)
        queue.get.call_count = nr_expected_calls

    def test_node_info(self, mock_path, zi_structure):
        node = MockLeafNode(())
        node._tree_manager = create_autospec(NodeTreeManager)
        node._tree_manager.path_to_info = {"path": "this_info"}

        info = zi_structure.nodes_to_info["/zi/debug/level"]
        node_info = NodeInfo(info)

        def fake_init_func(self_, *_, **__):
            self_._info = info

        with patch.object(
            NodeInfo,
            "__init__",
            return_value=None,
            side_effect=fake_init_func,
            autospec=True,
        ) as node_info_mock:
            node.node_info  # noqa: B018
            mock_path.assert_called_once()
            node_info_mock.assert_called_once_with(node_info, "this_info")


class TestWildCardOrPartialNode:
    @pytest.mark.asyncio()
    async def test_get(self, mock_path):
        node = MockWildcardOrPartialNode(())
        node._package_get_response = Mock(return_value="package_get_response")
        node._tree_manager = create_autospec(NodeTreeManager)
        node._tree_manager._session = create_autospec(Session)
        node.tree_manager.session.get_with_expression.return_value = _get_future(
            "get_response",
        )

        result = await node._get()
        mock_path.assert_called_once()
        node.tree_manager.session.get_with_expression.assert_called_once_with("path")
        node._package_get_response.assert_called_once_with("get_response")
        assert result == "package_get_response"

    @pytest.mark.asyncio()
    async def test_set(self, mock_path):
        node = MockWildcardOrPartialNode(())
        node._package_get_response = Mock(
            return_value="package_get_response",
            spec=True,
        )
        node._tree_manager = create_autospec(NodeTreeManager)
        node._tree_manager._session = create_autospec(Session)
        node.tree_manager.session.set_with_expression.return_value = _get_future(
            "get_response",
        )

        result = await node._set("set_value")
        mock_path.assert_called_once()
        node.tree_manager.session.set_with_expression.assert_called_once_with(
            AnnotatedValue(value="set_value", path=node.path),
        )
        node._package_get_response.assert_called_once_with("get_response")
        assert result == "package_get_response"

    @pytest.mark.asyncio()
    async def test_subscribe_raises(self):
        with pytest.raises(LabOneNotImplementedError):
            await MockWildcardOrPartialNode(()).subscribe()


class TestWildcardNode:
    @pytest.mark.parametrize(
        ("path_segments", "response", "prefixes", "have_timestamp"),
        [
            (
                ("dev1234", "oscs", WILDCARD, "freq"),
                [
                    AnnotatedValue(
                        value=i * 100,
                        path=join_path(("dev1234", "oscs", str(i), "freq")),
                    )
                    for i in range(8)
                ],
                [("dev1234", "oscs", str(i), "freq") for i in range(8)],
                False,
            ),
            (
                (WILDCARD,),
                [AnnotatedValue(path=join_path(("a", "b")), value=9, timestamp=42)],
                [("a",)],
                True,
            ),
            (
                (WILDCARD, "b"),
                [
                    AnnotatedValue(path=join_path(("a", "b")), value=9),
                    AnnotatedValue(path=join_path(("c", "b")), value=23),
                ],
                [("a", "b"), ("c", "b")],
                False,
            ),
            (
                (WILDCARD, "bound"),
                [
                    AnnotatedValue(path=join_path(("a", "b", "x")), value=9),
                    AnnotatedValue(path=join_path(("c", "b")), value=23),
                    AnnotatedValue(path=join_path(("a", "b", "z")), value=23),
                ],
                [("a", "b"), ("c", "b")],
                False,
            ),
            (
                (WILDCARD, "x"),
                [],
                [],
                False,
            ),
        ],
    )
    def test_package_get_response(
        self,
        path_segments,
        response,
        prefixes,
        have_timestamp,
    ):
        node = MockWildcardNode(())
        node._tree_manager = create_autospec(NodeTreeManager)
        node._tree_manager.parser.side_effect = lambda x: x
        node._path_segments = path_segments

        def fake_path_segments_to_node(path_segments):
            mock_node = MockNode(path_segments)
            mock_node._subtree_paths = "subtree_paths"
            return mock_node

        node._tree_manager.path_segments_to_node = Mock(
            side_effect=fake_path_segments_to_node,
            spec=True,
        )

        with patch("uuid.uuid4", return_value=1234, autospec=True):
            result_node = node._package_get_response(response)

        for r in response:
            node._tree_manager.parser.assert_any_call(r)

        for p in prefixes:
            node._tree_manager.path_segments_to_node.assert_any_call(p)

        assert result_node.path_segments == ("matches_1234_id",)
        assert result_node.tree_manager == node._tree_manager
        assert result_node._timestamp == (42 if have_timestamp else 0)

        for i, p in enumerate(sorted(prefixes)):
            assert (("matches_1234_id", str(i)), p) in result_node.path_aliases.items()

        assert result_node._value_structure == {a.path: a for a in response}
        assert list(result_node._subtree_paths.keys()) == [
            str(i) for i in range(len(prefixes))
        ]
        for i in range(len(prefixes)):
            assert result_node._subtree_paths[str(i)] == "subtree_paths"

    def test_try_generate_subnode(self):
        node = MockWildcardNode(("a", "b"))
        node._redirect = Mock(side_effect=lambda x: x)
        node._tree_manager = create_autospec(NodeTreeManager)
        node._tree_manager.path_segments_to_node.return_value = "subnode"

        result = node.try_generate_subnode("next")

        node._redirect.assert_called_once_with(("a", "b", "next"))
        node._tree_manager.path_segments_to_node.assert_called_once_with(
            ("a", "b", "next"),
        )
        assert result == "subnode"

    @pytest.mark.asyncio()
    async def test_wait_for_state_change(self, mock_path):
        node = MockWildcardNode(())
        node._tree_manager = create_autospec(NodeTreeManager)
        wait_mock = Mock(return_value=_get_future("awaited"), spec=True)

        def fake_raw_path_to_node(path):
            mock_node = MockWildcardNode(tuple(split_path(path)))
            mock_node.wait_for_state_change = wait_mock
            return mock_node

        node._tree_manager.raw_path_to_node.side_effect = fake_raw_path_to_node
        node._tree_manager._session = create_autospec(Session)
        node._tree_manager.session.list_nodes.return_value = _get_future(
            ["/a/b", "/a/c"],
        )

        await node.wait_for_state_change(value=1, invert="invert")

        mock_path.assert_called_once_with()
        node._tree_manager.session.list_nodes.assert_called_once_with("path")
        assert len(wait_mock.call_args_list) == 2
        wait_mock.assert_called_with(1, invert="invert")


class TestPartialNode:
    @pytest.mark.parametrize(
        ("path_segments", "response", "subtree_paths", "value_structure"),
        [
            (
                ("x", "y"),
                [
                    AnnotatedValue(path=join_path(("x", "y", "a")), value=1),
                    AnnotatedValue(path=join_path(("x", "y", "b")), value=2),
                ],
                {"a": [], "b": []},
                {
                    join_path(("x", "y", "a")): AnnotatedValue(
                        path=join_path(("x", "y", "a")),
                        value=1,
                    ),
                    join_path(("x", "y", "b")): AnnotatedValue(
                        path=join_path(("x", "y", "b")),
                        value=2,
                    ),
                },
            ),
            (
                (),
                [
                    AnnotatedValue(path=join_path(("x", "y", "a")), value=1),
                    AnnotatedValue(path=join_path(("x", "y", "b")), value=2),
                ],
                {"x": ["/y/a", "/y/b"]},
                {
                    join_path(("x", "y", "a")): AnnotatedValue(
                        path=join_path(("x", "y", "a")),
                        value=1,
                    ),
                    join_path(("x", "y", "b")): AnnotatedValue(
                        path=join_path(("x", "y", "b")),
                        value=2,
                    ),
                },
            ),
            (
                (),
                [
                    AnnotatedValue(path=join_path(("x",)), value=1),
                ],
                {"x": []},
                {
                    join_path(("x",)): AnnotatedValue(path=join_path(("x",)), value=1),
                },
            ),
        ],
    )
    def test_package_get_response(
        self,
        path_segments,
        response,
        subtree_paths,
        value_structure,
    ):
        node = MockPartialNode(path_segments)
        node._subtree_paths = subtree_paths
        node._tree_manager = create_autospec(NodeTreeManager)
        node._tree_manager.parser.side_effect = lambda x: x

        result_node = node._package_get_response(response)

        for r in response:
            node._tree_manager.parser.assert_any_call(r)

        assert isinstance(result_node, ResultNode)
        assert result_node._tree_manager == node._tree_manager
        assert result_node.path_segments == path_segments
        assert result_node.subtree_paths == subtree_paths
        assert result_node._value_structure == value_structure

    def test_try_generate_subnode(self):
        segments = ("a", "b")
        node = MockPartialNode(("a",))
        node._redirect = Mock(side_effect=lambda x: x)
        node._tree_manager = create_autospec(NodeTreeManager)
        node._tree_manager.path_segments_to_node.return_value = "subnode"
        node._tree_manager.find_substructure.return_value = {}

        deeper = node.try_generate_subnode(next_path_segment="b")

        node._redirect.assert_called_once_with(segments)
        node._tree_manager.find_substructure.assert_called_once_with(segments)
        node._tree_manager.path_segments_to_node.assert_called_once_with(segments)
        assert deeper == "subnode"

    def test_try_generate_subnode_invalid(self):
        segments = ("a", "b")
        node = MockPartialNode(("a",))
        node._redirect = Mock(side_effect=lambda x: x)
        node._tree_manager = create_autospec(NodeTreeManager)
        node._tree_manager.find_substructure.side_effect = LabOneInvalidPathError()

        with pytest.raises(LabOneInvalidPathError):
            node.try_generate_subnode(next_path_segment="b")

        node._redirect.assert_called_once_with(segments)
        node._tree_manager.find_substructure.assert_called_once_with(segments)

    def test_try_generate_subnode_wildcard(self):
        segments = ("a", "*")
        node = MockPartialNode(("a",))
        node._redirect = Mock(side_effect=lambda x: x)
        node._tree_manager = create_autospec(NodeTreeManager)
        node._tree_manager.path_segments_to_node.return_value = "subnode"
        node._tree_manager.find_substructure.side_effect = LabOneInvalidPathError()

        deeper = node.try_generate_subnode(next_path_segment=WILDCARD)

        node._redirect.assert_called_once_with(segments)
        node._tree_manager.find_substructure.assert_called_once_with(segments)
        node._tree_manager.path_segments_to_node.assert_called_once_with(segments)
        assert deeper == "subnode"

    @pytest.mark.asyncio()
    async def test_wait_for_state_change_raises(self):
        with pytest.raises(LabOneInappropriateNodeTypeError):
            await MockPartialNode(()).wait_for_state_change(2)
