import fnmatch
import typing as t

from labone.core import AnnotatedValue, ListNodesFlags, ListNodesInfoFlags
from labone.core.helper import LabOneNodePath
from labone.core.session import NodeInfo
from labone.core.subscription import DataQueue
from labone.core.value import Value
from labone.nodetree.helper import Session
from labone.nodetree.node import Node
from labone.sweeper.errors import SweeperLocalStateError


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
        self._paths_to_info = path_to_info
        self._memory = {path: AnnotatedValue(value=None, path=path) for path in path_to_info.keys()}  # bring all known paths into the memory
        self._set_parser = lambda x: x if set_parser is None else set_parser

    def sync_get(self, path: LabOneNodePath):
        try:
            return self._memory[path]
        except KeyError as e:
            raise SweeperLocalStateError(f"Trying to read sweeper path '{path}', which does not exist.") from e

    def sync_set(self, value: AnnotatedValue):
        if value.path not in self._memory:
            raise SweeperLocalStateError(
                f"Trying to write sweeper path '{value.path}', which does not exist.")

        node_info = self.sync_list_nodes_info()[value.path]
        if "Write" not in node_info["Properties"]:
            raise SweeperLocalStateError(
                f"Sweeper path '{value.path}' is not writeable.",
            )

        # TODO: additional checks for some nodes required?
        self._memory[value.path] = self._set_parser(value)
        return value

    def sync_list_nodes_info(
        self,
        path: LabOneNodePath = "*",
        flags: ListNodesInfoFlags | int = ListNodesInfoFlags.ALL,
    ) -> dict[LabOneNodePath, NodeInfo]:
        return {
            k: v for k, v in self._paths_to_info.items() if fnmatch.fnmatch(k, path)
        }

    async def list_nodes(
        self,
        path: LabOneNodePath = "*",
        flags: ListNodesFlags | int = ListNodesFlags.ABSOLUTE,
    ) -> list[LabOneNodePath]:
        if path[-1] != "*":
            path = path + "/*"
        return fnmatch.filter(self._paths_to_info.keys(), path)

    async def set(self, value: AnnotatedValue) -> AnnotatedValue:
        return self.sync_set(value)

    async def set_with_expression(self, value: AnnotatedValue) -> list[AnnotatedValue]:
        paths = await self.list_nodes(value.path)
        return [
            await self.set(AnnotatedValue(path=p, value=value.value)) for p in paths
        ]

    async def get(self, path: LabOneNodePath) -> AnnotatedValue:
        return self.sync_get(path)

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
        path: LabOneNodePath = "*",
        flags: ListNodesInfoFlags | int = ListNodesInfoFlags.ALL,
    ) -> dict[LabOneNodePath, NodeInfo]:
        return self.sync_list_nodes_info(path, flags)

    async def subscribe(
        self,
        path: LabOneNodePath,
        *,
        parser_callback: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
    ) -> DataQueue:
        raise NotImplementedError("LocalSession does not support subscriptions.")


NOT_SPECIFIED = object()


def _sync(node: Node, value: Value | None | NOT_SPECIFIED = NOT_SPECIFIED):
    """Non-async access to a node value."""
    session = node.tree_manager.session
    if not isinstance(session, LocalSession):
        raise RuntimeError(
            f"{__name__} can only be used on a local session, "
            f"which is inheritely synchronous. "
            f"It cannot be used on a {session}",
        )

    if value is NOT_SPECIFIED:
        return session.sync_get(node.path).value
    else:
        return session.sync_set(AnnotatedValue(value=value, path=node.path)).value


def sync_get(node: Node) -> Value:
    """Non-async access to a node value."""
    return _sync(node)


def sync_set(node: Node, value: Value) -> Value:
    """Non-async access to a node value."""
    return _sync(node, value)

