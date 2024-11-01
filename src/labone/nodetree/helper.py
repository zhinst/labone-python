"""Internal helper functions for node-tree.

Managing path-representation and structure of path-collections.
"""

from __future__ import annotations

import keyword
import typing as t

from labone.core import AnnotatedValue, ListNodesFlags, ListNodesInfoFlags
from labone.core.helper import LabOneNodePath

if t.TYPE_CHECKING:
    from typing_extensions import TypeAlias

    from labone.core.session import NodeInfo
    from labone.core.subscription import QueueProtocol


NormalizedPathSegment: TypeAlias = str
PATH_SEPERATOR = "/"
WILDCARD = "*"

T = t.TypeVar("T")


TreeProp = dict[LabOneNodePath, T]


class NestedDict(t.Protocol[T]):  # type: ignore[misc]
    """Protocol representing a nested dictionary structure."""

    def __getitem__(self, key: str) -> T | NestedDict[T]: ...

    # retyping dict method, because inheriting from non-protocal is prohibited
    def __setitem__(self, key: str, item: T | NestedDict) -> None: ...

    def keys(self) -> t.KeysView[str]:
        """..."""

    def items(self) -> t.ItemsView[str, T | NestedDict[T]]:
        """..."""

    def __iter__(self) -> t.Iterator[str]:
        """..."""


FlatPathDict: TypeAlias = dict[
    NormalizedPathSegment,
    list[list[NormalizedPathSegment]],
]


class Session(t.Protocol):
    """Interface for communication with a data-server."""

    def list_nodes(
        self,
        path: LabOneNodePath = "",
        *,
        flags: ListNodesFlags | int = ListNodesFlags.ABSOLUTE,
    ) -> t.Awaitable[list[LabOneNodePath]]:
        """List the nodes found at a given path."""
        ...

    def list_nodes_info(
        self,
        path: LabOneNodePath = "",
        *,
        flags: ListNodesInfoFlags | int = ListNodesInfoFlags.ALL,
    ) -> t.Awaitable[dict[LabOneNodePath, NodeInfo]]:
        """List the nodes and their information found at a given path."""
        ...

    def set(self, value: AnnotatedValue) -> t.Awaitable[AnnotatedValue]:
        """Set the value of a node."""
        ...

    def set_with_expression(
        self,
        value: AnnotatedValue,
    ) -> t.Awaitable[list[AnnotatedValue]]:
        """Set the value of all nodes matching the path expression."""
        ...

    def get(
        self,
        path: LabOneNodePath,
    ) -> t.Awaitable[AnnotatedValue]:
        """Get the value of a node."""
        ...

    def get_with_expression(
        self,
        path_expression: LabOneNodePath,
        flags: ListNodesFlags | int = ListNodesFlags.ABSOLUTE
        | ListNodesFlags.RECURSIVE
        | ListNodesFlags.LEAVES_ONLY
        | ListNodesFlags.EXCLUDE_STREAMING
        | ListNodesFlags.GET_ONLY,
    ) -> t.Awaitable[list[AnnotatedValue]]:
        """Get the value of all nodes matching the path expression."""
        ...

    async def subscribe(
        self,
        path: LabOneNodePath,
        *,
        parser_callback: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
        queue_type: type[QueueProtocol],
        get_initial_value: bool,
    ) -> QueueProtocol:
        """Register a new subscription to a node."""
        ...

    async def wait_for_state_change(
        self,
        path: LabOneNodePath,
        value: int,
        *,
        invert: bool = False,
    ) -> None:
        """Waits until the node has the expected state/value."""
        ...


class UndefinedStructure(dict):
    """Representing the state that the substructure of a node is not known."""


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
    if path_segments[0] == "":
        # this happens if the path started with '/'
        # ignore leading '/'
        first_item_index = 1
    return path_segments[first_item_index:]


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
    if keyword.iskeyword(path_segment):
        return path_segment + "_"

    return path_segment


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
