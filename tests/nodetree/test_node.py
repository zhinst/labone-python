import asyncio
import pickle
from io import BytesIO

import pytest
from labone.core import AnnotatedValue
from labone.core.subscription import DataQueue
from labone.nodetree.errors import (
    LabOneInappropriateNodeTypeError,
    LabOneInvalidPathError,
)
from labone.nodetree.helper import (
    join_path,
    nested_dict_access,
    pythonify_path_segment,
)
from labone.nodetree.node import (
    NodeInfo,
    NodeTreeManager,
    ResultNode,
    WildcardNode,
)

from tests.nodetree.conftest import (
    device_id,
    device_structure,
    get_result_node,
    get_serverless_manager,
    get_serverless_tree,
    get_tree,
    zi_get_responses_prop,
    zi_structure,
)


class TestMetaNode:
    @staticmethod
    def test_is_child_node():
        for node in [get_result_node(), get_serverless_tree()]:
            for subnode in node:
                if isinstance(subnode, node.__class__):
                    assert node.is_child_node(subnode)


class TestFindSubstructure:
    @staticmethod
    @pytest.mark.parametrize(
        ("path_segments"),
        [
            (()),
            (("zi",)),
            (("zi", "config")),
            (("zi", "config", "open")),
        ],
    )
    def test_valid_accesses(path_segments):
        zi = get_serverless_tree()
        main_structure = zi_structure.structure
        structure = zi.tree_manager.find_substructure(path_segments)

        assert (
            structure.keys() == nested_dict_access(path_segments, main_structure).keys()
        )

    @staticmethod
    def test_wrong_accesses():
        zi = get_serverless_tree()
        with pytest.raises(LabOneInvalidPathError):
            zi.tree_manager.find_substructure(("zi", "config", "open", "too_long"))
        with pytest.raises(LabOneInvalidPathError):
            zi.tree_manager.find_substructure(("wrong", "access"))
        with pytest.raises(LabOneInvalidPathError):
            zi.tree_manager.find_substructure(("*",))

    @staticmethod
    def test_repeated_access():
        zi = get_serverless_tree()
        node = zi.config
        access1 = zi.tree_manager.find_substructure(node.path_segments)
        access2 = zi.tree_manager.find_substructure(node.path_segments)
        access3 = zi.tree_manager.find_substructure(node.path_segments)

        assert access1 == access2 == access3


class TestNodetreeManager:
    @staticmethod
    def test_raw_path_to_node():
        zi = get_serverless_tree()
        manager = zi.tree_manager
        node = manager.raw_path_to_node("/zi/config")

        assert node == zi.config
        assert set(node.subtree_structure.keys()) == {"open", "port"}

    @staticmethod
    def test_path_segments_to_node():
        zi = get_serverless_tree()
        manager = zi.tree_manager
        node = manager.path_segments_to_node(("zi", "config"))

        assert node == zi.config
        assert set(node.subtree_structure.keys()) == {"open", "port"}

    @staticmethod
    def test_hide_prefix():
        manager = get_serverless_manager()
        tree = manager.construct_nodetree(hide_kernel_prefix=False)
        zi = manager.construct_nodetree(hide_kernel_prefix=True)

        assert tree.zi == zi

    @staticmethod
    def test_init():
        manager = NodeTreeManager(
            session=None,
            path_to_info=zi_structure.nodes_to_info,
            parser=lambda x: x,
        )
        assert manager.path_to_info == zi_structure.nodes_to_info
        assert manager.paths == zi_structure.paths


def test_repr():
    manager = get_serverless_manager()
    for path in ["zi", "zi/config", "zi/config/open", "zi/*/open"]:
        node = manager.raw_path_to_node(path)
        assert path in repr(node)  # path should be contained in repr and str
        assert path in str(node)


def test_deprecated():
    zi = get_serverless_tree()
    with pytest.warns(DeprecationWarning):
        _ = zi.raw_tree

    with pytest.warns(DeprecationWarning):
        assert zi.raw_tree == zi.path_segments


def test_dir():
    zi = get_serverless_tree()
    assert "config" in dir(zi)


class TestResultNode:
    @staticmethod
    def test_generate_subnode():
        r = get_result_node()
        sub_node = r["config/open"]
        assert sub_node == r.config.open

    @staticmethod
    def test_generate_subnode_invalid():
        result_node = get_result_node()
        with pytest.raises(LabOneInvalidPathError):
            _ = result_node.invalid

    @staticmethod
    def test_leaf_values():
        result = get_result_node()
        assert result.config.port.value == 8004
        assert result.debug.level.value == 3

    @staticmethod
    def test_getitem_too_deep():
        result_node = get_result_node()
        with pytest.raises(LabOneInvalidPathError):
            result_node["config/open/too/long"]

    @staticmethod
    def test_str_repr():
        result_node = get_result_node()
        assert isinstance(repr(result_node), str)
        assert isinstance(str(result_node), str)

    @staticmethod
    @pytest.mark.asyncio()
    async def test_pickling():
        zi = await get_tree()
        result_node = await zi.debug.level()

        buffer = BytesIO()
        pickle.dump(result_node, buffer)

        buffer.seek(0)
        unpickled_obj = pickle.load(buffer)  # noqa: S301

        assert unpickled_obj == result_node

    @staticmethod
    def test_dir():
        result_node = get_result_node()
        assert "config" in dir(result_node)

    @staticmethod
    def test_contains():
        result_node = get_result_node()
        assert "config" in result_node
        assert result_node.config in result_node

        subnode = result_node.debug

        assert "level" in subnode
        assert subnode.level in subnode

    @staticmethod
    def test_wildcard():
        result_node = get_result_node()
        with pytest.raises(LabOneInvalidPathError):
            result_node["*"]

    @staticmethod
    def test_not_callable():
        result_node = get_result_node()
        with pytest.raises(LabOneInappropriateNodeTypeError):
            result_node()


class TestNode:
    @staticmethod
    def test_str_repr():
        zi = get_serverless_tree()

        # test callable
        assert isinstance(repr(zi), str)
        assert isinstance(str(zi), str)

    @staticmethod
    @pytest.mark.asyncio()
    async def test_eq():
        zi = get_serverless_tree()
        node1 = zi.config

        assert zi.config.port == zi.config.port
        assert zi.config.port == node1.port

        assert zi != node1
        assert zi.config.port != zi.config.open

        other_trees_zi = await get_tree()
        assert zi != other_trees_zi
        assert zi.path == other_trees_zi.path

    @staticmethod
    @pytest.mark.asyncio()
    async def test_hash():
        zi = get_serverless_tree()
        node1 = zi.config

        assert hash(zi) == hash(zi)
        assert hash(zi) != hash(node1)

        other_trees_zi = await get_tree()
        assert hash(zi) != hash(other_trees_zi)

    @staticmethod
    def test_contains():
        zi = get_serverless_tree()
        assert "config" in zi
        assert zi.config in zi

        node1 = zi.config

        for child in node1:
            assert child in node1

    @staticmethod
    def test_keyword_handling():
        zi = get_serverless_tree()

        # use unchecked wildcard-paths for testing keywords
        node = zi["*"].in_.if_
        assert node.path_segments == ("zi", "*", "in", "if")

    @staticmethod
    @pytest.mark.asyncio()
    async def test_get():
        zi = await get_tree()
        result = await zi.config.port()
        assert result.value == 8004

        result = await zi.config.port()
        assert result.value == 8004

    @staticmethod
    @pytest.mark.asyncio()
    async def test_set():
        zi = await get_tree()
        result = await zi.config.port("uvw")
        assert result.value == "uvw"

        result = await zi.config.port("klm")
        assert result.value == "klm"

    @staticmethod
    def test_dir():
        zi = get_serverless_tree()

        for extension in ["visible", "connected"]:
            assert pythonify_path_segment(extension) in zi.devices.__dir__()

        for node in [zi, zi.devices, zi.mds.groups[0], zi.clockbase]:
            for extension in node.subtree_structure:
                assert pythonify_path_segment(extension) in node.__dir__()

    @staticmethod
    @pytest.mark.asyncio()
    async def test_wait_for_change_state_loop():
        zi = await get_tree()
        queue: DataQueue = await zi.debug.level.subscribe()

        async def wait_and_set(time, value):
            nonlocal queue
            await asyncio.sleep(time)
            await queue.put(value)

        node = zi.debug.level

        # already correct value
        await node.wait_for_state_change(3, timeout=0.02)

        # also satisfied from the beginning
        await node.wait_for_state_change(5, timeout=0.02, invert=True)

        # simulate remote change situation
        await asyncio.gather(
            node.wait_for_state_change(4, timeout=0.1),
            wait_and_set(0.05, AnnotatedValue(path="zi/debug/level", value=4)),
        )

        with pytest.raises(asyncio.exceptions.TimeoutError):
            await node.wait_for_state_change(5, timeout=0.02)


class TestLeafNode:
    @staticmethod
    def test_try_generate_subnode():
        zi = get_serverless_tree()
        leaf = zi.debug.level
        with pytest.raises(LabOneInvalidPathError):
            leaf.try_generate_subnode(next_path_segment="any")

    @staticmethod
    @pytest.mark.asyncio()
    async def test_subscribe():
        zi = await get_tree()
        queue = await zi.debug.level.subscribe()
        assert isinstance(queue, DataQueue)

    @staticmethod
    def test_node_info():
        zi = get_serverless_tree()
        assert isinstance(zi.debug.level.node_info, NodeInfo)


class TestPartialNode:
    @staticmethod
    def test_package_get_response():
        zi = get_serverless_tree()

        node = zi.config
        # answer to '/zi/config',
        # (make use of fact that all leafs are direct children of '/zi/config')
        response = [zi_get_responses_prop[subnode.path] for subnode in node]

        result_node = node._package_get_response(response)
        assert isinstance(result_node, ResultNode)
        assert len(result_node) == 2
        assert result_node._path_aliases == {}
        assert result_node.path == node.path

    @staticmethod
    def test_wrong_subnode():
        zi = get_serverless_tree()
        with pytest.raises(LabOneInvalidPathError):
            _ = zi.not_existing

    @staticmethod
    @pytest.mark.asyncio()
    async def test_wait_for_state_change():
        zi = get_serverless_tree()
        with pytest.raises(LabOneInappropriateNodeTypeError):
            await zi.wait_for_state_change(value=1, timeout=0.1)


class TestWildcardPartialNode:
    @staticmethod
    @pytest.mark.asyncio()
    async def test_get():
        zi = await get_tree()
        result = await zi["*"]()
        assert isinstance(result, ResultNode)


class TestWildcardNode:
    @staticmethod
    def test_package_get_response():
        device = get_serverless_tree(nodes_to_info=device_structure.nodes_to_info)
        node = device.oscs["*"].freq
        # example answer to wildcard get-request
        response = [
            AnnotatedValue(i * 100, join_path((device_id, "oscs", str(i), "freq")))
            for i in range(8)
        ]

        result_node = node._package_get_response(response)
        assert isinstance(result_node, ResultNode)
        assert len(result_node) == 8
        assert result_node._path_aliases != {}
        expected_paths = [f"/{device_id}/oscs/{i}/freq" for i in range(8)]

        # test redirection
        for value_or_node in result_node:
            # explicitly not mixing property 'path' of AnnotatedValue and ResultNode
            if isinstance(value_or_node, AnnotatedValue):
                assert value_or_node.path in expected_paths
            else:
                assert value_or_node.path in expected_paths

    @staticmethod
    def test_subindexing():
        zi = get_serverless_tree()

        # no error
        node = zi["*"].whatever.you.want
        assert isinstance(node, WildcardNode)

    @staticmethod
    @pytest.mark.asyncio()
    async def test_get():
        zi = await get_tree()
        node = zi["*"].level

        # already correct value
        await node.wait_for_state_change(3, timeout=0.02)

        with pytest.raises(asyncio.exceptions.TimeoutError):
            await node.wait_for_state_change(5, timeout=0.02)

    @staticmethod
    @pytest.mark.asyncio()
    async def test_set():
        zi = await get_tree()
        node = zi["*"].level

        await node(3)
