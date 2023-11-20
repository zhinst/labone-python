import fnmatch
import typing as t

from labone.core import AnnotatedValue, ListNodesFlags, ListNodesInfoFlags
from labone.core.helper import LabOneNodePath
from labone.core.session import NodeInfo
from labone.core.subscription import DataQueue
from labone.nodetree.helper import Session


class LocalSession(Session):
    """Stateful session that does not interact with any device.

    Initializing a nodetree with
    this kind of session will lead to a working nodetree, which may be useful in
    if a convenient tree-structure is helpful for other purposes than setting
    properties in some device.
    """

    def __init__(
        self,
        path_to_info: dict[LabOneNodePath, NodeInfo],
        set_parser: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
    ):
        self._memory = {}
        self._paths_to_info = path_to_info
        self._set_parser = lambda x: x if set_parser is None else set_parser

    async def list_nodes(
        self,
        path: LabOneNodePath = "*",
        flags: ListNodesFlags | int = ListNodesFlags.ABSOLUTE,
    ) -> list[LabOneNodePath]:
        if path[-1] != "*":
            path = path + "/*"
        return fnmatch.filter(self._paths_to_info.keys(), path)

    async def set(self, value: AnnotatedValue) -> AnnotatedValue:
        self._memory[value.path] = self._set_parser(value)
        return value

    async def set_with_expression(self, value: AnnotatedValue) -> list[AnnotatedValue]:
        paths = await self.list_nodes(value.path)
        return [
            await self.set(AnnotatedValue(path=p, value=value.value)) for p in paths
        ]

    async def get(self, path: LabOneNodePath) -> AnnotatedValue:
        return self._memory[path]

    async def get_with_expression(
        self,
        path_expression: LabOneNodePath,
        flags: ListNodesFlags
        | int = ListNodesFlags.ABSOLUTE
        | ListNodesFlags.RECURSIVE
        | ListNodesFlags.LEAVES_ONLY
        | ListNodesFlags.EXCLUDE_STREAMING
        | ListNodesFlags.GET_ONLY,
    ) -> list[AnnotatedValue]:
        paths = await self.list_nodes(path_expression)
        return [await self.get(p) for p in paths]

    async def list_nodes_info(
        self,
        path: LabOneNodePath,
        flags: ListNodesInfoFlags | int = ListNodesInfoFlags.ALL,
    ) -> dict[LabOneNodePath, NodeInfo]:
        return {
            k: v for k, v in self._paths_to_info.items() if fnmatch.fnmatch(k, path)
        }

    async def subscribe(
        self,
        path: LabOneNodePath,
        *,
        parser_callback: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
    ) -> DataQueue:
        raise NotImplementedError()
