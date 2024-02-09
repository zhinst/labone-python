

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import typing as t
from typing import Any, TypeAlias
import warnings
from abc import ABC, abstractmethod
from functools import cached_property

from deprecation import deprecated
from capnp import KjException
from labone.core.subscription import DataQueue
from labone.core.value import AnnotatedValue, Value
from labone.node_info import NodeInfo
from labone.nodetree.errors import (
    LabOneInappropriateNodeTypeError,
    LabOneInvalidPathError,
    LabOneNotImplementedError,
)
from labone.nodetree.helper import (
    WILDCARD,
    NestedDict,
    NormalizedPathSegment,
    Session,
    TreeProp,
    UndefinedStructure,
    build_prefix_dict,
    join_path,
    normalize_path_segment,
    pythonify_path_segment,
    split_path,
)

if t.TYPE_CHECKING:
    from labone.core.helper import LabOneNodePath
    from labone.core.session import NodeInfo as NodeInfoType
    from labone.core.subscription import QueueProtocol
    from labone.nodetree.enum import NodeEnum

T = t.TypeVar("T")
NUMBER_PLACEHOLDER = "N"

def stringify_id(raw_id) -> str:
    return ''.join([hex(e)[-2:] for e in raw_id])


@dataclass
class Range:
    start: int
    end: int

def get_range(node_reader):
    try:
        return Range(node_reader.range.start, node_reader.range.end)
    except KjException:
        return None


NodeId: TypeAlias = str

def get_field_if_present(capnp_reader, field_name, else_value=None)-> None | t.Any:
    try:
        return getattr(capnp_reader, field_name)
    except:
        return else_value

class NewNode:
    def from_capnp(capnp_reader, tree_manager: NodeTreeManager2):
        try:
            sub_reader = capnp_reader.bareNode
        except:
            return RangeNode.from_capnp(capnp_reader.rangeNode, tree_manager)
        return BareNode.from_capnp(sub_reader, tree_manager)

class BareNode:
    def __init__(self, tree_manager: NodeTreeManager2, id_: NodeId, name: str, segment_to_subnode: dict[str, NodeId], info):
        self.tree_manager = tree_manager
        self.id_ = id_
        self.name = name
        self.segment_to_subnode = segment_to_subnode
        self.info = info
        self.path_segments = None

    def from_capnp(capnp_reader, tree_manager: NodeTreeManager2):
        sub_nodes = get_field_if_present(capnp_reader, "subNodes", {})
        return BareNode(
            tree_manager,
            id_=stringify_id(capnp_reader.id),
            name=capnp_reader.name,
            segment_to_subnode={e.name : stringify_id(e.id) for e in sub_nodes},
            info=get_field_if_present(capnp_reader, "info"),
        )


class RangeNode:
    def __init__(self, tree_manager: NodeTreeManager2, id_: NodeId, range_: Range, segment_to_subnode: dict[str, NodeId], info):
        self.tree_manager = tree_manager
        self.id_ = id_
        self.range = range_
        self.segment_to_subnode = segment_to_subnode
        self.info = info
        self.path_segments = None

    def from_capnp(capnp_reader, tree_manager: NodeTreeManager2):
        sub_nodes = get_field_if_present(capnp_reader, "subNodes", {})
        return RangeNode(
            tree_manager,
            id_=stringify_id(capnp_reader.id),
            range_=get_range(capnp_reader),
            segment_to_subnode={e.name : stringify_id(e.id) for e in sub_nodes},
            info=get_field_if_present(capnp_reader, "info"),
        )


class ConcreteNode:
    """Instance of node where indexes of the parametrizations are fixed."""
    def __init__(self, node: BareNode, *, parametrizations=None):
        self.node = node
        self.parametrizations: list[int] = parametrizations or []

    def __getattr__(self, __name: str) -> Any:
        sub_id = self.node.segment_to_subnode[__name]
        sub_node = self.node.tree_manager.id_to_segment[sub_id]
        sub_node.path_segments = (*self.node.path_segments, __name)  # inform subnode about its path
        return ConcreteNode(sub_node, parametrizations=self.parametrizations.copy())
    
    def __getitem__(self, nr: int):
        path_segment = normalize_path_segment(nr)

        sub_id = self.node.segment_to_subnode[NUMBER_PLACEHOLDER]
        sub_node = self.node.tree_manager.id_to_segment[sub_id]
        assert isinstance(sub_node, RangeNode), self.node 
        assert sub_node.range.start <= nr and nr <= sub_node.range.end, "Out of range"

        sub_node.path_segments = (*self.node.path_segments, path_segment)  # inform subnode about its path

        new_parametrization = self.parametrizations.copy()
        new_parametrization.append(nr)
        return ConcreteNode(sub_node, parametrizations=new_parametrization)
    
    def __repr__(self) -> str:
        return f"Node2({self.path}, {self.range}, {self.segment_to_subnode}, {self.info})"

    async def __call__(self, *args):
        if len(args) == 0:
            print(f"get {self.path}")
        if len(args) == 1:
            print(f"set {self.path} to {args[0]}")

    @property
    def path(self):
        return join_path(self.concrete_path_segments)
    
    @property
    def concrete_path_segments(self):
        parameter_index = 0
        for segment in self.node.path_segments:
            if segment == NUMBER_PLACEHOLDER:
                yield str(self.parametrizations[parameter_index])
                parameter_index += 1
            else:
                yield segment


class NodeTreeManager2:
    def __init__(self, session: Session, nodes : list[BareNode]):
        self.session = session
        self.nodes = [NewNode.from_capnp(e, self) for e in nodes]
        self.id_to_segment = {e.id_: e for e in self.nodes}
        self.root = self.nodes[0]
        self.root.path_segments = (self.root.name,)

    async def create(session: Session):
        nodes = await session.get_nodes()
        return NodeTreeManager2(session, nodes)

