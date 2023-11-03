"""Helper functions for node-tree.

Managing path-representation and structure of path-collections.
"""

from __future__ import annotations

import keyword
import typing as t

from labone.core import AnnotatedValue, ListNodesFlags, ListNodesInfoFlags
from labone.core.helper import LabOneNodePath

if t.TYPE_CHECKING:
    from typing_extensions import TypeAlias  # pragma: no cover

    from labone.core.session import NodeInfo  # pragma: no cover
    from labone.core.subscription import DataQueue  # pragma: no cover


NormalizedPathSegment: TypeAlias = str
PATH_SEPERATOR = "/"
WILDCARD = "*"

T = t.TypeVar("T")


TreeProp = t.Dict[LabOneNodePath, T]


class NestedDict(t.Protocol[T]):  # type: ignore[misc]
    """Protocol representing a nested dictionary structure."""

    def __getitem__(self, key: str) -> T | NestedDict[T]:
        ...  # pragma: no cover

    # retyping dict method, because inheriting from non-protocal is prohibited
    def __setitem__(self, key: str, item: T | NestedDict) -> None:
        ...  # pragma: no cover

    def keys(self) -> t.KeysView[str]:
        """..."""
        ...  # pragma: no cover

    def items(self) -> t.ItemsView[str, T | NestedDict[T]]:
        """..."""
        ...  # pragma: no cover

    def __iter__(self) -> t.Iterator[str]:
        """..."""
        ...  # pragma: no cover


FlatPathDict: TypeAlias = t.Dict[
    NormalizedPathSegment,
    t.List[t.List[NormalizedPathSegment]],
]


class Session(t.Protocol):
    """Interface for communication with a data-server."""

    async def list_nodes(
        self,
        path: LabOneNodePath,
        flags: ListNodesFlags | int = ListNodesFlags.ABSOLUTE,
    ) -> list[LabOneNodePath]:
        """List the nodes found at a given path.

        Args:
            path: A string representing the path where the nodes are to be listed.
                Value is case-insensitive.

                Supports asterix (*) wildcards, except in the place of node path forward
                slashes.
            flags: The flags for modifying the returned nodes.

        Returns:
            A list of strings representing the nodes.

            Returns an empty list when `path` does not match any nodes or the nodes
                matching `path` does not fit into given `flags` criteria.
        """
        ...  # pragma: no cover

    async def list_nodes_info(
        self,
        path: LabOneNodePath,
        flags: ListNodesInfoFlags | int = ListNodesInfoFlags.ALL,
    ) -> dict[LabOneNodePath, NodeInfo]:
        """List the nodes and their information found at a given path.

        Args:
            path: A string representing the path where the nodes are to be listed.
                Value is case insensitive.

                Supports asterix (*) wildcards, except in the place of node path
                forward slashes.

            flags: The flags for modifying the returned nodes.

        Returns:
            A python dictionary where absolute node paths are keys and their
                information are values.

            An empty dictionary when `path` does not match any nodes or the nodes
                matching `path` does not fit into given `flags` criteria.
        """
        ...  # pragma: no cover

    async def set(self, value: AnnotatedValue) -> AnnotatedValue:  # noqa: A003
        """Set the value of a node.

        Args:
            value: Value to be set. The annotated value must contain a
                LabOne node path and a value. (The path can be relative or
                absolute.)

        Returns:
            Acknowledged value from the device.

        Example:
            >>> await session.set(AnnotatedValue(path="/zi/debug/level", value=2)
        """
        ...  # pragma: no cover

    async def set_with_expression(self, value: AnnotatedValue) -> list[AnnotatedValue]:
        """Set the value of all nodes matching the path expression.

        A path expression is a labone node path. The difference to a normal
        node path is that it can contain wildcards and must not be a leaf node.
        In short it is a path that can match multiple nodes. For more information
        on path expressions see the `list_nodes()` function.

        If an error occurs while fetching the values no value is returned but
        the first first exception instead.

        Args:
            value: Value to be set. The annotated value must contain a
                LabOne path expression and a value.

        Returns:
            Acknowledged value from the device.

        Example:
            >>> ack_values = await session.set_with_expression(
                    AnnotatedValue(path="/zi/*/level", value=2)
                )
            >>> print(ack_values[0])
        """
        ...  # pragma: no cover

    async def get(
        self,
        path: LabOneNodePath,
    ) -> AnnotatedValue:
        """Get the value of a node.

        The node can either be passed as an absolute path, starting with a leading
        slash and the device id (e.g. "/dev123/demods/0/enable") or as relative
        path (e.g. "demods/0/enable"). In the latter case the device id is
        automatically added to the path by the server. Note that the
        orchestrator/ZI kernel always requires absolute paths (/zi/about/version).

        Args:
            path: LabOne node path (relative or absolute).

        Returns:
            Annotated value of the node.

        Example:
            >>> await session.get('/zi/devices/visible')
        """
        ...  # pragma: no cover

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
        """Get the value of all nodes matching the path expression.

        A path expression is a labone node path. The difference to a normal
        node path is that it can contain wildcards and must not be a leaf node.
        In short it is a path that can match multiple nodes. For more information
        on path expressions see the `list_nodes()` function.

        If an error occurs while fetching the values no value is returned but
        the first first exception instead.

        Args:
            path_expression: LabOne path expression.
            flags: The flags used by the server (list_nodes()) to filter the
                nodes.

        Returns:
            Annotated values from the nodes matching the path expression.

        Example:
            >>> values = await session.get_with_expression("/zi/*/level")
            >>> print(values[0])
        """
        ...  # pragma: no cover

    async def subscribe(
        self,
        path: LabOneNodePath,
        parser_callback: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
    ) -> DataQueue:
        """Register a new subscription to a node.

        Registers a new subscription to a node on the kernel/server. All
        updates to the node will be pushed to the returned data queue.

        Note:
            An update is triggered by the device itself and does not
            exclusively mean a change in the value of the node. For example
            a set request from any client will also trigger an update event.

        It is safe to have multiple subscriptions to the same path. However
        in most cases it is more efficient to fork (DataQueue.fork) an
        existing DataQueue rather then registering a new subscription at the
        server. This is because the kernel/server will send the update events
        to every registered subscription independently, causing additional
        network overhead.

        Args:
            path: String representing the path of the node to be streamed.
                Currently does not support wildcards in the path.
            parser_callback: Function to bring values obtained from
                data-queue into desired format. This may involve parsing
                them or putting them into an enum.

        Returns:
            An instance of the DataQueue class. This async queue will receive
            all update events for the subscribed node.

        Example:
            >>> data_sink = await session.subscribe("/zi/devices/visible")
            >>> newly_detected_device = await data_sink.get()
        """
        ...  # pragma: no cover


class UndefinedStructure(dict):
    """A tree-structure allowing all kinds of indexation."""

    def __getitem__(self, _: object) -> UndefinedStructure:
        return UndefinedStructure()

    def __eq__(self, other: object) -> bool:
        """All instances should have equal meaning."""
        return self.__class__ == other.__class__


def nested_dict_access(
    key_chain: t.Iterable,
    nested_dict: NestedDict[T],
) -> NestedDict[T] | T:
    """Recursively going deeper in a nested dictionary.

    Args:
        key_chain: keys to recursively index the nested dictionary
        nested_dict: nested dictionaries

    Returns:
        The result at this position of the nested dictionary.

    Raises:
        KeyError: if any of the keys is not appropriate or the sequence of keys is
            too long.

    """
    current_dict: NestedDict[T] | T = nested_dict
    try:
        for key in key_chain:
            current_dict = current_dict[key]  # type: ignore[index]
    except KeyError as e:
        if current_dict:
            raise KeyError from e
        msg = (
            f"{nested_dict} cannot be indexed with {key_chain}, because key-sequence"
            f" is to long and hits a leaf early"
        )
        raise KeyError(msg) from e
    return current_dict


def join_path(path_segments: t.Iterable[NormalizedPathSegment]) -> LabOneNodePath:
    """Join path in a well-defined manner.

    Node that a leading seperator is always added.

    Args:
        path_segments: Segments to join.

    Returns:
        One joint path.
    """
    return PATH_SEPERATOR + PATH_SEPERATOR.join(path_segments)


def split_path(path: LabOneNodePath) -> list[NormalizedPathSegment]:
    """Split path in a well-defined manner.

    A leading seperator is ignored.

    Args:
        path: Path to be split.

    Returns:
        Segments of which the path consists.
    """
    if path == "/":
        return []
    path_segments = path.split(PATH_SEPERATOR)
    first_item_index = 0
    if path_segments[0] == "":  #
        # this happens if the path started with '/'
        # ignore leading '/'
        first_item_index = 1
    return path_segments[first_item_index:]


def get_prefix(path: LabOneNodePath, segment_count: int) -> LabOneNodePath:
    """Get a path consisting only of the first n segments of the original path.

    Args:
        path: Path, the prefix should be taken from.
        segment_count: Number of segments, which should be included. If there are
            not enough segments, all available ones are included.

    Returns:
        A path, similar to the given one, but bounded in terms of number of segments.
    """
    segments = split_path(path)
    first_segments = segments[:segment_count]
    return join_path(first_segments)


def normalize_path_segment(path_segment: str | int) -> NormalizedPathSegment:
    """Bring segment into a standard form.

    - no integers, but only strings
    - '_' in reserved names ignored

    Args:
        path_segment: Segment of a path to be normalized.

    Returns:
        The segment, following the described formatting standards.

    """
    return str(path_segment).lower().rstrip("_")


def pythonify_path_segment(path_segment: NormalizedPathSegment) -> str:
    """Try to bring segment into a form, which can be used as an attribute for a node.

    - add '_' at end of reserved names
    - make clear that numbers should be used with indexing e.g. [0] instead of node.0

    Args:
        path_segment: Segment in the usual representation.

    Returns:
        Path segment in pythonic representation.

    """
    if path_segment.isdigit():
        return str(path_segment)

    if keyword.iskeyword(path_segment):
        return path_segment + "_"

    return path_segment


def paths_to_nested_dict(paths: list[LabOneNodePath]) -> NestedDict[dict]:
    """Builds a nested dictionary structure out of a collection of paths.

    Args:
        paths: List of paths.

    Returns:
        A tree-like dictionary structure representing the paths.

    """
    return _build_nested_dict_recursively([split_path(path) for path in paths])


def _build_nested_dict_recursively(
    suffix_list: list[list[NormalizedPathSegment]],
) -> NestedDict[dict]:
    local_result = build_prefix_dict(suffix_list)

    # recursively solve the emerging sub-problems
    return {
        first_segment: _build_nested_dict_recursively(local_suffixes)
        for first_segment, local_suffixes in local_result.items()
    }


def build_prefix_dict(
    suffix_list: list[list[NormalizedPathSegment]],
) -> FlatPathDict:
    """Builds a dictionary prefix-to-remainder from list of paths.

    Build a dictionary of first-segment to list of path-suffixes
    like {prefix1: [suffix1, suffix2], prefix2: [], prefix3: [suffix3]}
    where all suffixes are guaranteed to be non-empty.

    Note:
        This function assumes following preconditions:
        - No empty lists in main list, checkable with
        >>> for l in suffix_list:
        >>>     assert l
        - No two paths are identical, so each member of suffix_list is unique.

        After this function, for each value of the resulting dictionary,
        the same criteria apply as postconditions.

    # no two paths are identical


    Args:
        suffix_list: list of paths (is split form)

    Returns:
        Dictionary of first-segment -> list of path-suffixes starting with it

    """
    result: FlatPathDict = {}
    for path in suffix_list:
        first_segment = path[0]
        path_suffix = path[1:]

        if first_segment not in result:
            result[first_segment] = []
        if path_suffix:
            result[first_segment].append(path_suffix)

    return result
