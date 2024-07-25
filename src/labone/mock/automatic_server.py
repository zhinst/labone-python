"""Partially predifined behaviour for HPK mock.

This class provides basic Hpk mock functionality by taking over some usually
desired tasks. With that in place, the user may inherit from this class
in order to further specify behavior, without having to start from scratch.
Even if some of the predefined behaviour is not desired, the implementation
can give some reference on how an individual mock server can be implemented.


Already predefined behaviour:

    * Simulating state for get/set:
        A dictionary is used to store the state of the mock server.
        Get and set will access this dictionary.
    * Answering list_nodes(_info) via knowledge of the tree structure:
        Given a dictionary of paths to node info passed in the constructor,
        the list_nodes(_info) methods will be able to answer accordingly.
    * Reducing get_with_expression/set_with_expression to multiple get/set:
        As the tree structure is known, the get_with_expression/set_with_expression
        methods can be implemented by calling the get/set methods multiple times.
    * Managing subscriptions and passing all changes into the queues:
        The subscriptions are stored and on every change, the new value is passed
        into the queues.
    * Adding chronological timestamps to responses:
        The server answers need timestamps to the responsis in any case.
        By using the monotonic clock, the timestamps are added automatically.

"""

from __future__ import annotations

import asyncio
import fnmatch
import time
import typing as t
from dataclasses import dataclass

from labone.core import ListNodesFlags, ListNodesInfoFlags
from labone.core.errors import LabOneCoreError
from labone.core.value import (
    AnnotatedValue,
    Value,
)
from labone.mock.errors import LabOneMockError
from labone.mock.session import LabOneServerBase, Subscription
from labone.node_info import NodeInfo

if t.TYPE_CHECKING:
    from labone.core.helper import LabOneNodePath
    from labone.core.session import NodeInfo as NodeInfoType


@dataclass
class PathData:
    """Data stored for each path in the mock server."""

    value: Value
    info: NodeInfo
    streaming_handles: list[Subscription]


class AutomaticLabOneServer(LabOneServerBase):
    """Predefined behaviour for HPK mock.

    Args:
        paths_to_info: Dictionary of paths to node info. (tree structure)
    """

    def __init__(
        self,
        paths_to_info: dict[LabOneNodePath, NodeInfoType],
    ) -> None:
        super().__init__()
        # storing state and tree structure, info and subscriptions
        # set all existing paths to 0.
        common_prefix_raw = (
            next(iter(paths_to_info.keys())).split("/") if paths_to_info else []
        )
        self._common_prefix: str | None = (
            f"/{common_prefix_raw[1]}"
            if len(common_prefix_raw) > 1 and common_prefix_raw[1] != ""
            else None
        )
        self.memory: dict[LabOneNodePath, PathData] = {}
        for path, given_info in paths_to_info.items():
            info = NodeInfo.plain_default_info(path=path)
            info.update({"Type": "Integer (64 bit)"})  # for mock, int nodes are default
            info.update(given_info)
            self.memory[path] = PathData(
                value=0,
                info=NodeInfo(info),
                streaming_handles=[],
            )
            if self._common_prefix and not path.startswith(self._common_prefix):
                self._common_prefix = None

    def get_timestamp(self) -> int:
        """Create a realisitc timestamp.

        Call this function to obtain a timestamp for some response.
        As a internal clock is used, subsequent calls will return
        increasing timestamps.

        Returns:
            Timestamp in nanoseconds.
        """
        return time.monotonic_ns()

    def _sanitize_path(self, path: LabOneNodePath) -> LabOneNodePath:
        """Sanatize the path.

        Removes trailing slashes and replaces empty path with root path.

        Args:
            path: Path to sanatize.

        Returns:
            Sanatized path.
        """
        if self._common_prefix and not path.startswith("/"):
            return f"{self._common_prefix}/{path}"
        return path

    async def list_nodes_info(
        self,
        path: LabOneNodePath = "",
        *,
        flags: ListNodesInfoFlags | int = ListNodesInfoFlags.ALL,  # noqa: ARG002
    ) -> dict[LabOneNodePath, NodeInfoType]:
        """Predefined behaviour for list_nodes_info.

        Uses knowledge of the tree structure to answer.

        Warning:
            Flags will be ignored in this implementation. (TODO)
            For now, the behaviour is equivalent to
            ListNodesFlags.RECURSIVE | ListNodesFlags.ABSOLUTE

        Args:
            path: Path to narrow down which nodes should be listed. Omitting
                the path will list all nodes by default.
            flags: Flags to control the behaviour of the list_nodes_info method.

        Returns:
            Dictionary of paths to node info.
        """
        return {
            p: self.memory[p].info.as_dict for p in await self.list_nodes(path=path)
        }

    async def list_nodes(
        self,
        path: LabOneNodePath = "",
        *,
        flags: ListNodesFlags | int = ListNodesFlags.ABSOLUTE,  # noqa: ARG002
    ) -> list[LabOneNodePath]:
        """Predefined behaviour for list_nodes.

        Uses knowledge of the tree structure to answer.

        Warning:
            Flags will be ignored in this implementation. (TODO)
            For now, the behaviour is equivalent to
            ListNodesFlags.RECURSIVE | ListNodesFlags.ABSOLUTE

        Args:
            path: Path to narrow down which nodes should be listed. Omitting
                the path will list all nodes by default.
            flags: Flags to control the behaviour of the list_nodes method.

        Returns:
            List of paths.
        """
        if path in [""]:
            return []
        return [
            p
            for p in self.memory
            if fnmatch.fnmatch(p, path)
            or fnmatch.fnmatch(p, path + "*")
            or fnmatch.fnmatch(p, path + "/*")
            or p == path
        ]

    async def get(self, path: LabOneNodePath) -> AnnotatedValue:
        """Predefined behaviour for get.

        Look up the path in the internal dictionary.

        Args:
            path: Path of the node to get.

        Returns:
            Corresponding value.
        """
        path = self._sanitize_path(path)
        try:
            value = self.memory[path].value
        except KeyError as e:
            msg = f"Path {path} not found in mock server. Cannot get it."
            raise LabOneMockError(msg) from e
        response = AnnotatedValue(path=path, value=value)
        response.timestamp = self.get_timestamp()
        return response

    async def get_with_expression(
        self,
        path_expression: LabOneNodePath,
        flags: ListNodesFlags | int = ListNodesFlags.ABSOLUTE  # noqa: ARG002
        | ListNodesFlags.RECURSIVE
        | ListNodesFlags.LEAVES_ONLY
        | ListNodesFlags.EXCLUDE_STREAMING
        | ListNodesFlags.GET_ONLY,
    ) -> list[AnnotatedValue]:
        """Predefined behaviour for get_with_expression.

        Find all nodes associated with the path expression
        and call get for each of them.

        Args:
            path_expression: Path expression to get.
            flags: Flags to control the behaviour of the get_with_expression method.

        Returns:
            List of values, corresponding to nodes of the path expression.
        """
        return [await self.get(p) for p in await self.list_nodes(path=path_expression)]

    async def set(self, value: AnnotatedValue) -> AnnotatedValue:
        """Predefined behaviour for set.

        Updates the internal dictionary. A set command is considered
        as an update and will be distributed to all registered subscription handlers.

        Args:
            value: Value to set.

        Returns:
            Acknowledged value.
        """
        value.path = self._sanitize_path(value.path)
        if value.path not in self.memory:
            msg = f"Path {value.path} not found in mock server. Cannot set it."
            raise LabOneCoreError(msg)
        self.memory[value.path].value = value.value

        if not self.memory[value.path].info.writable:
            msg = f"Path {value.path} is not writeable."
            raise LabOneCoreError(msg)

        response = value
        response.timestamp = self.get_timestamp()

        if self.memory[value.path].streaming_handles:
            # sending updated value to subscriptions
            await asyncio.gather(
                *[
                    handle.send_value(response)
                    for handle in self.memory[response.path].streaming_handles
                ],
            )
        return response

    async def set_with_expression(self, value: AnnotatedValue) -> list[AnnotatedValue]:
        """Predefined behaviour for set_with_expression.

        Finds all nodes associated with the path expression
        and call set for each of them.

        Args:
            value: Value to set.

        Returns:
            List of acknowledged values, corresponding to nodes of the path expression.
        """
        result = [
            await self.set(AnnotatedValue(value=value.value, path=p))
            for p in await self.list_nodes(value.path)
        ]
        if not result:
            msg = f"No node found matching path '{value.path}'."
            raise LabOneCoreError(msg)
        return result

    async def subscribe(self, subscription: Subscription) -> None:
        """Predefined behaviour for subscribe.

        Stores the subscription. Whenever an update event happens
        they are distributed to all registered handles,

        Args:
            subscription: Subscription object containing information on
                where to distribute updates to.
        """
        self.memory[self._sanitize_path(subscription.path)].streaming_handles.append(
            subscription,
        )
