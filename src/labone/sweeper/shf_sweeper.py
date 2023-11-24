import asyncio
import inspect
import json
import textwrap
import typing as t
from functools import partial
from pathlib import Path

from labone.core.helper import LabOneNodePath
from labone.core.value import Value
from labone.nodetree import construct_nodetree
from labone.nodetree.helper import join_path
from labone.nodetree.node import Node, PartialNode
from labone.sweeper.constants import _SHF_SAMPLE_RATE
from labone.sweeper.local_session import LocalSession, sync_get, sync_set


class Sweeper(PartialNode):
    _shf_sample_rate = _SHF_SAMPLE_RATE

    def __init__(self, *, device_tree, model_node: Node):
        super().__init__(
            tree_manager=model_node.tree_manager,
            path_segments=model_node.path_segments,
            subtree_paths=model_node.subtree_paths,
            path_aliases=model_node.path_aliases,
        )
        self._device_tree = device_tree

    @classmethod
    async def create(cls, session):
        path_to_info = json.loads(
            Path.open(Path(__file__).parent / "sweeper_nodes.json").read(),
        )
        device_tree = await construct_nodetree(
            session=session,
            hide_kernel_prefix=False,
            use_enum_parser=True,
        )
        model_node = await construct_nodetree(
            session=LocalSession(path_to_info),
            hide_kernel_prefix=False,
            use_enum_parser=False,
        )
        instance = cls(device_tree=device_tree, model_node=model_node)

        return instance
