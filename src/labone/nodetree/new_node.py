from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
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
from time import time_ns, sleep

if t.TYPE_CHECKING:
    from labone.core.helper import LabOneNodePath
    from labone.core.session import NodeInfo as NodeInfoType
    from labone.core.subscription import QueueProtocol
    from labone.nodetree.enum import NodeEnum

T = t.TypeVar("T")
NUMBER_PLACEHOLDER = "N"


class TimingTest:
    def __init__(self, title) -> None:
        self.title = title
        self.start_time = 0

    def __enter__(self):
        self.start_time = time_ns()

    def __exit__(self, exc_type, exc_value, traceback):
        print(f"{self.title}: {(time_ns()-self.start_time)/1000000} ms")


def stringify_id(raw_id) -> str:
    return "".join([hex(e)[-2:] for e in raw_id])


class NodeProperty(Enum):
    READ = 1
    WRITE = 2
    SETTING = 3


@dataclass
class Option:
    value: int
    aliases: list[str]
    description: str | None

    def from_capnp(capnp_struct):
        return Option(
            value=capnp_struct.value,
            aliases=capnp_struct.aliases,
            description=get_field_if_present(capnp_struct, "description"),
        )


@dataclass
class NodeInfo2:
    description: str
    properties: list[NodeProperty]
    type: type[object]
    unit: str
    options: list[Option]

    def from_capnp(capnp_struct):
        options = get_field_if_present(capnp_struct, "options")
        if options is not None:
            options = [Option.from_capnp(e) for e in options]

        return NodeInfo2(
            description=capnp_struct.description,
            properties=capnp_struct.properties,
            type=capnp_struct.type,
            unit=capnp_struct.unit,
            options=options,
        )


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


def get_field_if_present(capnp_reader, field_name, else_value=None) -> None | t.Any:
    try:
        return getattr(capnp_reader, field_name)
    except:
        return else_value


async def create_node_tree(session: Session):
    nodes = await session.get_nodes()
    id_to_segment = {e.id: e for e in nodes}
    return Segment(session, id_to_segment, nodes[0], [], [])


class Segment:
    def __init__(
        self,
        session: Session,
        id_to_segment: dict[int, t.Any],
        capnp_struct,
        parametrization: list[int],
        previous_path_segments: list[NormalizedPathSegment],
    ) -> None:
        self.session = session
        self.id_to_segment = id_to_segment
        self.capnp_struct = capnp_struct
        self.parametrization = parametrization
        self.path_segments = previous_path_segments + [capnp_struct.name]

    def __getattr__(self, item: str) -> Any:
        sub_id = self.segment_to_subnode[item]
        sub_node = self.id_to_segment[sub_id]
        return Segment(
            session=self.session,
            id_to_segment=self.id_to_segment,
            capnp_struct=sub_node,
            parametrization=self.parametrization.copy(),
            previous_path_segments=self.path_segments,
        )
    
    def __getitem__(self, nr: int):
        path_segment = normalize_path_segment(nr)

        sub_id = self.segment_to_subnode[NUMBER_PLACEHOLDER]
        sub_node = self.id_to_segment[sub_id]

        assert 0 <= nr and nr <= sub_node.rangeEnd, "Out of range"

        new_parametrization = self.parametrization.copy()
        new_parametrization.append(nr)
        return Segment(
            session=self.session,
            id_to_segment=self.id_to_segment,
            capnp_struct=sub_node,
            parametrization=new_parametrization,
            previous_path_segments=self.path_segments,
        )

    @cached_property
    def segment_to_subnode(self):
        return {e.name: e.id for e in self.capnp_struct.subNodes}
    
    @property
    def info(self):
        return self.capnp_struct.info#NodeInfo2.from_capnp(self.capnp_struct.info)
    
    @property
    def path(self):
        return join_path(self.concrete_path_segments)
    
    @property
    def concrete_path_segments(self):
        parameter_index = 0
        for segment in self.path_segments:
            if segment == NUMBER_PLACEHOLDER:
                yield str(self.parametrization[parameter_index])
                parameter_index += 1
            else:
                yield segment

    def __repr__(self) -> str:
        return self.path

    async def __call__(self, *args):
        if len(args) == 0:
            return await self.session.get(self.path)
        if len(args) == 1:
            return await self.session.set(AnnotatedValue(path=self.path, value=args[0]))
