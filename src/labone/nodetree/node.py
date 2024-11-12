"""LabOne object-based node-tree implementation.

This module contains the core functionality of the node-tree. It provides
the classes for the different types of nodes, the node info and the
NodeTreeManager, which is the interface to the server.
"""

from __future__ import annotations

import asyncio
import typing as t
import warnings
import weakref
from abc import ABC, abstractmethod
from functools import cached_property

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


class NodeTreeManager:
    """Managing relation of a node tree and its underlying session.

    Acts as a factory for nodes and holds the reference to the underlying
    session. I holds a nested dictionary representing the structure of the
    node tree.

    Args:
        session: Session from which the node tree data is retrieved.
        path_to_info: Result of former server-call, representing structure of
            tree plus information about each node.
        parser: Function, which is used to parse incoming values. It may do this
            in a path-specific manner.
        hide_kernel_prefix: Enter a trivial first path-segment automatically.
                E.g. having the result of this function in a variable `tree`
                `tree.debug.info` can be used instead of `tree.device1234.debug.info`.
                Setting this option makes working with the tree easier.
    """

    def __init__(
        self,
        *,
        session: Session,
        path_to_info: dict[LabOneNodePath, NodeInfoType],
        parser: t.Callable[[AnnotatedValue], AnnotatedValue],
        hide_kernel_prefix: bool = True,
    ):
        self._session = session
        self.path_to_info = path_to_info
        self._parser = parser
        self._hide_kernel_prefix = hide_kernel_prefix

        self._cache_path_segments_to_node: weakref.WeakValueDictionary[
            tuple[NormalizedPathSegment, ...],
            Node,
        ] = weakref.WeakValueDictionary()
        self._cache_find_substructure: (dict)[
            tuple[NormalizedPathSegment, ...],
            NestedDict[list[list[NormalizedPathSegment]] | dict],
        ] = {}

        self._root_prefix: tuple[str, ...] = ()
        self._paths_as_segments: list[list[NormalizedPathSegment]] = []
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.add_nodes_with_info(path_to_info)

    def add_nodes_with_info(
        self,
        path_to_info: dict[LabOneNodePath, NodeInfoType],
    ) -> None:
        """Add new nodes to the tree.

        This function will not check that the added nodes are valid,
        so it should be used with care. Normally, it is not necessary to
        add nodes manually, because they will be acquired automatically
        on construction.

        Warning:
            The root prefix may change, if the prefix was hidden before but is now not
            unique any more. The access to the nodes in the tree may change.
            This will trigger a warning.

        Args:
            path_to_info: Describing the new paths and the associated
                information.
        """
        # dict prevents duplicates
        self.path_to_info.update(path_to_info)

        self._paths_as_segments = [split_path(path) for path in self.path_to_info]

        # already explored structure is forgotten and will be re-explored on demand.
        # this is necessary, because the new nodes might be in the middle of the tree
        self._cache_find_substructure = {}

        # type casting to allow assignment of more general type into the dict
        self._partially_explored_structure: NestedDict[
            list[list[NormalizedPathSegment]] | dict
        ] = build_prefix_dict(
            self._paths_as_segments,
        )  # type: ignore[assignment]

        # root prefix may change, if the prefix was hidden before but is now not
        # unique any more
        old_root_prefix = self._root_prefix
        has_common_prefix = len(self._partially_explored_structure.keys()) == 1

        if not self._hide_kernel_prefix or not has_common_prefix:
            self._root_prefix = ()
        else:
            common_prefix = next(iter(self._partially_explored_structure.keys()))
            self._root_prefix = (common_prefix,)

        if self._root_prefix != old_root_prefix:
            msg = (
                f"Root prefix changed from '{join_path(old_root_prefix)}'"
                f" to '{join_path(self._root_prefix)}'. "
                f"This means in order to index the same node as before, "
                f"use {'.'.join(['root',*old_root_prefix,'(...)'])} "
                f"instead of {'.'.join(['root',*self._root_prefix,'(...)'])}."
            )
            warnings.warn(msg, Warning, stacklevel=1)

    def add_nodes(self, paths: list[LabOneNodePath]) -> None:
        """Add new nodes to the tree.

        This function will not check that the added nodes are valid,
        so it should be used with care. Normally, it is not necessary to
        add nodes manually, because they will be acquired automatically
        on construction.

        Warning:
            The root prefix may change, if the prefix was hidden before but is now not
            unique any more. The access to the nodes in the tree may change.
            This will trigger a warning.

        Args:
            paths: Paths of the new nodes.
        """
        self.add_nodes_with_info(
            {p: NodeInfo.plain_default_info(path=p) for p in paths},
        )

    def find_substructure(
        self,
        path_segments: tuple[NormalizedPathSegment, ...],
    ) -> NestedDict[list[list[NormalizedPathSegment]] | dict]:
        """Find children and explore the node structure lazily as needed.

        This function must be used by all nodes to find their children.
        This ensures efficient caching of the structure and avoids
        unnecessary lookups.

        Args:
            path_segments: Segments describing the path, for which the children
                should be found.

        Returns:
            Nested dictionary, representing the structure of the children.

        Raises:
            LabOneInvalidPathError: If the path segments are invalid.
        """
        if path_segments in self._cache_find_substructure:
            return self._cache_find_substructure[path_segments]

        # base case
        if not path_segments:
            # this is not worth adding to the cache, so just return it
            return self._partially_explored_structure

        # solving recursively, taking advantage of caching
        # makes usual indexing of lower nodes O(1)
        sub_solution = self.find_substructure(path_segments[:-1])
        segment = path_segments[-1]

        try:
            sub_solution[segment]
        except KeyError as e:
            if segment == WILDCARD:
                msg = (
                    f"Cannot find structure for a path containing a wildcard,"
                    f"however, `find_structure` was called with "
                    f"{join_path(path_segments)}"
                )
                raise LabOneInvalidPathError(msg) from e

            msg = (
                f"Path '{join_path(path_segments)}' is illegal, because '{segment}' "
                f"is not a viable extension of '{join_path(path_segments[:-1])}'. "
                f"It does not correspond to any existing node."
                f"\nViable extensions would be {list(sub_solution.keys())}"
            )
            raise LabOneInvalidPathError(msg) from e

        # explore structure deeper on demand
        # the path not being cached implies this is the first time
        # this substructure is explored.
        # So we know it is a list of paths, which we now build into a prefix dict.
        sub_solution[segment] = build_prefix_dict(
            sub_solution[segment],  # type: ignore[arg-type]
        )

        result: NestedDict[list[list[NormalizedPathSegment]] | dict] = sub_solution[
            segment
        ]  # type: ignore[assignment]
        self._cache_find_substructure[path_segments] = result
        return result

    def raw_path_to_node(
        self,
        path: LabOneNodePath,
    ) -> Node:
        """Convert a LabOne node path into a node object.

        Caches node-objects and enforces thereby a singleton pattern.
        Only one node-object per path.

        Args:
            path: Path, for which a node object should be provided.

        Returns:
            Node-object corresponding to the path.

        Raises:
            LabOneInvalidPathError:
                In no subtree_paths are given and the path is invalid.
        """
        return self.path_segments_to_node(tuple(split_path(path)))

    def path_segments_to_node(
        self,
        path_segments: tuple[NormalizedPathSegment, ...],
    ) -> Node:
        """Convert a tuple of path-segments into a node object.

        Caches node-objects and enforces thereby a singleton pattern.
        Only one node-object per path.

        Args:
            path_segments: Segments describing the path, for which a node object
                should be provided.

        Returns:
            Node-object corresponding to the path.

        Raises:
            LabOneInvalidPathError: In no subtree_paths are given and the path
                is invalid.
        """
        if path_segments in self._cache_path_segments_to_node:
            return self._cache_path_segments_to_node[path_segments]

        result = Node.build(tree_manager=self, path_segments=path_segments)
        self._cache_path_segments_to_node[path_segments] = result
        return result

    def __hash__(self) -> int:
        return id(self)

    @property
    def parser(self) -> t.Callable[[AnnotatedValue], AnnotatedValue]:
        """Parser for values received from the server."""
        return self._parser

    @property
    def session(self) -> Session:
        """Underlying Session to the server."""
        return self._session

    @property
    def root(self) -> Node:
        """Create the root-node of the tree.

        Depending on the hide_kelnel_prefix-setting of the
        NodeTreeManager, the root will either be '/' or
        the directly entered device, like '/dev1234'

        Returns:
            Root-node of the tree.
        """
        return self.path_segments_to_node(self._root_prefix)


class MetaNode(ABC):
    """Basic functionality of all nodes.

    This class provides common behavior for all node classes, both normal nodes
    and result nodes. This includes the traversal of the tree and the generation
    of sub-nodes.

    Args:
        tree_manager: Interface managing the node-tree and the corresponding
            session.
        path_segments: A tuple describing the path.
        subtree_paths: Structure, defining which sub-nodes exist.
            May contain a Nested dictionary or a list of paths. If a list is
            passed, a prefix-to-suffix-dictionary will be created out of it.
    """

    def __init__(
        self,
        *,
        tree_manager: NodeTreeManager,
        path_segments: tuple[NormalizedPathSegment, ...],
        subtree_paths: (
            NestedDict[list[list[NormalizedPathSegment]] | dict] | UndefinedStructure
        ),
    ):
        self._tree_manager = tree_manager
        self._path_segments = path_segments
        self._subtree_paths = subtree_paths

    @abstractmethod
    def __getattr__(self, next_segment: str) -> MetaNode | AnnotatedValue:
        """Access sub-node or value.

        ```python
        node.demods  # where 'demods' is the next segment to enter
        ```

        Args:
            next_segment: Segment, with which the current path should be extended

        Returns:
            New node-object, representing the extended path, or plain value,
            if came to a leaf-node of a result.
        """
        ...

    @abstractmethod
    def __getitem__(self, path_extension: str | int) -> MetaNode | AnnotatedValue:
        """Go one or multiple levels deeper into the tree structure.

        The primary purpose of this operator is to allow accessing subnodes that
        are numbers (e.g. /0, /1, ...). The attribute access operator (dot) does
        not allow this, because numbers are not valid identifiers in Python.

        However, this operator can deal with a number of different scenarios:

        - simple path extensions:

            ```python
            node['deeper']
            ```

        - path indexing:

            ```python
            node['deeper/path']
            ```

        - numeric indexing:

            ```python
            node[0]
            ```

        - wildcards (placeholder for multiple path-extensions):

            ```python
            node['*']
            ```

        - combinations of all that:

            ```python
            node['deeper/*/path/0']
            ```


        All these implementations are equivalent:

        ```python
        node['mds/groups/0']
        node['mds']['groups'][0]
        node.mds.groups[0]
        ```

        Args:
            path_extension: path, number or wildcard.

        Returns: New node-object, representing the extended path, or plain value,
            if came to a leaf-node.
        """
        ...

    def __iter__(self) -> t.Iterator[MetaNode | AnnotatedValue]:
        """Iterating through direct sub-nodes.

        The paths are traversed in a sorted manner, providing a clear order.
        This is particularly useful when iterating through numbered child nodes,
        such as /0, /1, ... or alphabetically sorted child nodes.

        Returns:
            Sub-nodes iterator.
        """
        for segment in sorted(self._subtree_paths.keys()):
            yield self[segment]

    def __len__(self) -> int:
        """Number of direct sub-nodes."""
        return len(self._subtree_paths.keys())

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self!s})"

    def __str__(self) -> str:
        return self.path

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, MetaNode):  # pragma: no cover
            msg = (
                f"'<' not supported between instances of "
                f"'{type(self)}' and '{type(other)}'"
            )  # pragma: no cover
            raise TypeError(msg)  # pragma: no cover
        return self.path < other.path

    def is_child_node(
        self,
        child_node: MetaNode | t.Sequence[NormalizedPathSegment],
    ) -> bool:
        """Checks if a node is a direct child node of this node.

        Children of children (etc.) will not be counted as direct children.
        The node itself is also not counted as its child.

        Args:
            child_node: Potential child node.

        Returns:
            Boolean if passed node is a child node.
        """
        path_segments = (
            child_node.path_segments
            if isinstance(child_node, MetaNode)
            else tuple(child_node)
        )

        return (
            len(path_segments) == len(self.path_segments) + 1
            and path_segments[: len(self.path_segments)] == self.path_segments
            and path_segments[-1] in self._subtree_paths
        )

    @property
    def tree_manager(self) -> NodeTreeManager:
        """Get interface managing the node-tree and the corresponding session."""
        return self._tree_manager

    @property
    def path(self) -> LabOneNodePath:
        """The LabOne node path, this node corresponds to."""
        return join_path(self._path_segments)

    @property
    def path_segments(self) -> tuple[NormalizedPathSegment, ...]:
        """The underlying segments of the path, this node corresponds to."""
        return self._path_segments

    @property
    def raw_tree(self) -> tuple[NormalizedPathSegment, ...]:
        """The underlying segments of the path, this node corresponds to.

        Deprecated: use 'path_segments' instead.
        """
        return self.path_segments

    @property
    def subtree_paths(
        self,
    ) -> NestedDict[list[list[NormalizedPathSegment]] | dict] | UndefinedStructure:
        """Structure defining which sub-nodes exist."""
        return self._subtree_paths

    @property
    def children(self) -> list[str]:
        """List of direct sub-node names."""
        return [pythonify_path_segment(p) for p in self._subtree_paths]


class ResultNode(MetaNode):
    """Representing values of a get-request in form of a tree.

    When issuing a get-request on a partial or wildcard node, the server will
    return a list of AnnotatedValues for every leaf-node in the subtree.
    This class adds the same object oriented interface as the normal nodes, when
    hitting a leaf node the result is returned.

    This allows to work with the results of a get-request in the same way as
    with the normal nodes. If needed one can still iterate through all the
    results. `results` will return an iterator over all results, not only the
    direct children.

    Args:
        tree_manager:
            Interface managing the node-tree and the corresponding session.
        path_segments: A tuple describing the path.
        subtree_paths:
            Structure, defining which sub-nodes exist.
        value_structure:
            Storage of the values at the leaf-nodes. They will be
            returned once the tree is traversed.
        timestamp:
            The time the results where created.
    """

    def __init__(
        self,
        tree_manager: NodeTreeManager,
        path_segments: tuple[NormalizedPathSegment, ...],
        subtree_paths: NestedDict[list[list[NormalizedPathSegment]] | dict],
        value_structure: TreeProp,
        timestamp: int,
    ):
        super().__init__(
            tree_manager=tree_manager,
            path_segments=path_segments,
            subtree_paths=subtree_paths,
        )
        self._value_structure = value_structure
        self._timestamp = timestamp

    def __getattr__(self, next_segment: str) -> ResultNode | AnnotatedValue:
        """Access sub-node or value.

        Args:
            next_segment: Segment, with which the current path should be extended.

        Returns:
            New node-object, representing the extended path, or plain value,
            if came to a leaf-node.
        """
        return self.try_generate_subnode(normalize_path_segment(next_segment))

    def __getitem__(self, path_extension: str | int) -> ResultNode | AnnotatedValue:
        """Go one or multiple levels deeper into the tree structure.

        The primary purpose of this operator is to allow accessing subnodes that
        are numbers (e.g. /0, /1, ...). The attribute access operator (dot) does
        not allow this, because numbers are not valid identifiers in Python.

        However, this operator can deal with a number of different scenarios:

        - simple path extensions:
            >>> node['deeper']
        - path indexing:
            >>> node['deeper/path']
        - numeric indexing:
            >>> node[0]
        - wildcards (placeholder for multiple path-extensions):
            >>> node['*']
        - combinations of all that:
            >>> node['deeper/*/path/0']

        Args:
            path_extension: path, number or wildcard.

        Returns: New node-object, representing the extended path, or plain value,
            if came to a leaf-node.

        Raises:
            LabOneInvalidPathError: If path is invalid.

        """
        relative_path_segments = split_path(str(path_extension))
        current_node = self
        try:
            for path_segment in relative_path_segments:
                current_node = current_node.try_generate_subnode(
                    normalize_path_segment(path_segment),
                )  # type: ignore[assignment]

        except AttributeError as e:
            msg = (
                f"Path {join_path((*self.path_segments,*relative_path_segments))} "
                f"is invalid, because {current_node.path} "
                f"is already a leaf-node."
            )
            raise LabOneInvalidPathError(msg) from e

        return current_node

    def __dir__(self) -> t.Iterable[str]:
        """Show valid subtree-extensions in hints.

        Returns:
            Iterator of valid dot-access identifier.

        """
        return self.children + list(super().__dir__())

    def __contains__(self, item: str | int | ResultNode | AnnotatedValue) -> bool:
        """Checks if a path-segment or node is an immediate sub-node of this one."""
        if isinstance(item, ResultNode):
            return self.is_child_node(item)
        if isinstance(item, AnnotatedValue):
            return self.is_child_node(split_path(item.path))
        return normalize_path_segment(item) in self._subtree_paths

    def __call__(self, *_, **__) -> None:
        """Not supported on the Result node.

        Showing an error to express result-nodes can't be get/set.

        Raises:
            LabOneInappropriateNodeTypeError: Always.
        """
        msg = (
            "Trying to get/set a result node. This is not possible, because "
            "result nodes represents values of a former get-request. "
            "To interact with a device, make sure to operate on normal nodes."
        )
        raise LabOneInappropriateNodeTypeError(msg)

    def __str__(self) -> str:
        value_dict = {
            path: self._value_structure[path].value
            for path in self._value_structure
            if path.startswith(self.path)
        }
        return (
            f"{self.__class__.__name__}('{self.path}', time: #{self._timestamp}, "
            f"data: {value_dict})"
        )

    def __repr__(self) -> str:
        return f"{self!s} -> {[str(k) for k in self._subtree_paths]}"

    def results(self) -> t.Iterator[AnnotatedValue]:
        """Iterating through all results.

        The difference to the normal iterator is that this iterator will iterate
        through all results, not only the direct children. This is useful when
        iterating through results of a wildcard or partial node.

        Returns:
            Results iterator.
        """
        for path, value in self._value_structure.items():
            if path.startswith(self.path):
                yield value

    def try_generate_subnode(
        self,
        next_path_segment: NormalizedPathSegment,
    ) -> ResultNode | AnnotatedValue:
        """Provides nodes for the extended path or the original values for leafs.

        Will fail if the resulting Path is ill-formed.

        Args:
            next_path_segment: Segment, with which the current path should be
                extended.

        Returns:
            New node-object, representing the extended path, or plain value,
            if came to a leaf-node.

        Raises:
            LabOneInvalidPathError: If the extension leads to an invalid path or if it
                is tried to use wildcards in ResultNodes.
        """
        extended_path = (*self._path_segments, next_path_segment)
        try:
            next_subtree = self.subtree_paths[next_path_segment]
        except KeyError as e:
            if next_path_segment == WILDCARD:
                msg = (
                    f"Wildcards '*' are not allowed in a tree representing "
                    f"measurement results. However, it was tried to extend {self.path} "
                    f"with a wildcard."
                )
                raise LabOneInvalidPathError(msg) from e

            # this call checks whether the path is in the nodetree itself
            # if this is not the case, this line will raise the appropriate error
            # otherwise, the path is in the tree, but not captured in this particular
            # result node.
            self.tree_manager.find_substructure(extended_path)

            msg = (
                f"Path '{join_path(extended_path)}' is not captured in this "
                "result node. It corresponds to an existing node, but the "
                "request producing this result collection was make such that "
                "this result is not included. Change either the request or "
                "access a different node."
            )
            raise LabOneInvalidPathError(msg) from e

        # exploring deeper tree stucture if it is not aleady known
        if isinstance(next_subtree, list):  # pragma: no cover
            next_subtree = build_prefix_dict(next_subtree)

        deeper_node = ResultNode(
            tree_manager=self.tree_manager,
            path_segments=extended_path,
            subtree_paths=next_subtree,  # type: ignore[arg-type]
            value_structure=self._value_structure,
            timestamp=self._timestamp,
        )

        if not next_subtree:
            # give value instead of subnode if already at a leaf
            # generate hypothetical node in order to apply normal behavior
            return self._value_structure[deeper_node.path]
        return deeper_node


class Node(MetaNode, ABC):
    """Single node in the object-based node tree.

    The child nodes of each node can be accessed either by attribute or by item.

    The core functionality of each node is the overloaded call operator.
    Making a call gets the value(s) for that node. Passing a value to the call
    operator will set that value to the node on the device. Calling a node that
    is not a leaf (wildcard or partial node) will return/set the value on every
    node that matches it.

    Warning:
        Setting a value to a non-leaf node will try to set the value of all
        nodes that matches that node. It should therefore be used with great care
        to avoid unintentional changes.

    In addition to the call operator every node has a `wait_for_state_change`
    function that can be used to wait for a node to change state.

    Leaf nodes also have a `subscribe` function that can be used to subscribe to
    changes in the node. For more information on the subscription functionality
    see the documentation of the `subscribe` function or the `DataQueue` class.
    """

    def __getattr__(self, next_segment: str) -> Node:
        """Access sub-node.

        ```python
        node.demods  # where 'demods' is the next segment to enter
        ```

        Args:
            next_segment: Segment, with which the current path should be extended

        Returns:
            New node-object, representing the extended path, or plain value,
            if came to a leaf-node.
        """
        return self.try_generate_subnode(normalize_path_segment(next_segment))

    def __getitem__(self, path_extension: str | int) -> Node:
        """Go one or multiple levels deeper into the tree structure.

        The primary purpose of this operator is to allow accessing subnodes that
        are numbers (e.g. /0, /1, ...). The attribute access operator (dot) does
        not allow this, because numbers are not valid identifiers in Python.

        However, this operator can deal with a number of different scenarios:

        - simple path extensions:

            ```python
            node['deeper']
            ```

        - path indexing:

            ```python
            node['deeper/path']
            ```

        - numeric indexing:

            ```python
            node[0]
            ```

        - wildcards (placeholder for multiple path-extensions):

            ```python
            node['*']
            ```

        - combinations of all that:

            ```python
            node['deeper/*/path/0']
            ```


        All these implementations are equivalent:

        ```python
        node['mds/groups/0']
        node['mds']['groups'][0]
        node.mds.groups[0]
        ```

        Args:
            path_extension: path, number or wildcard.

        Returns: New node-object, representing the extended path
        """
        relative_path_segments = split_path(str(path_extension))
        current_node = self

        for path_segment in relative_path_segments:
            current_node = current_node.try_generate_subnode(
                normalize_path_segment(path_segment),
            )

        return current_node

    def __eq__(self, other: object) -> bool:
        return (
            other.__class__ == self.__class__
            and self.path_segments == other.path_segments  # type:ignore[attr-defined]
            and self.tree_manager == other.tree_manager  # type:ignore[attr-defined]
        )

    def __hash__(self) -> int:
        return hash((self.path, hash(self.__class__), hash(self._tree_manager)))

    def __dir__(self) -> t.Iterable[str]:
        """Show valid subtree-extensions in hints.

        Returns:
            Iterator of valid dot-access identifier.

        """
        return self.children + list(super().__dir__())

    def __contains__(self, item: str | int | Node) -> bool:
        """Checks if a path-segment or node is an immediate sub-node of this one.

        Args:
            item: To be checked this is among the child-nodes. Can be called with
                either a node, or a plain identifier/number, which would be used
                to identify the child.

        Returns:
            If item describes/is a valid subnode.

        Example:
            >>> if "debug" in node:         # implicit call to __contains__
            >>>     print(node["debug"])    # if contained, indexing is valid
            >>>     print(node.debug)       # ... as well as dot-access

            Nodes can also be used as arguments. In this example, it is asserted
            that all subnodes are "contained" in the node.
            >>> for subnode in node:        # implicit call to __iter__
            >>>     assert subnode in node  # implicit call to __contains__
        """
        if isinstance(item, Node):
            return self.is_child_node(item)
        return normalize_path_segment(item) in self._subtree_paths

    def __call__(
        self,
        value: Value | None = None,
    ) -> t.Awaitable[AnnotatedValue | ResultNode]:
        """Call with or without a value for setting/getting the node.

        Args:
            value: optional value, which is set to the node. If it is omitted,
                a get request is triggered instead.

        Returns:
            The current value of the node is returned either way. If a set-request
                is triggered, the new value will be given back. In case of non-leaf
                nodes, a node-structure representing the results of all sub-paths is
                returned.

        Raises:
            OverwhelmedError: If the kernel is overwhelmed.
            BadRequestError: If the path is not writeable or readable.
            UnimplementedError: If the get or set request is not supported
                by the server.
            InternalError: If an unexpected internal error occurs.
            LabOneCoreError: If something else went wrong that can not be
                mapped to one of the other errors.
        """
        if value is None:
            return self._get()

        return self._set(value)

    @abstractmethod
    def _get(
        self,
    ) -> t.Awaitable[AnnotatedValue | ResultNode]: ...

    @abstractmethod
    def _set(
        self,
        value: Value,
    ) -> t.Awaitable[AnnotatedValue | ResultNode]: ...

    @classmethod
    def build(
        cls,
        *,
        tree_manager: NodeTreeManager,
        path_segments: tuple[NormalizedPathSegment, ...],
    ) -> Node:
        """Construct a matching subnode.

        Useful for creating a node, not necessarily knowing what kind of node it
        should be. This factory method will choose the correct one.

        Args:
            tree_manager: Interface managing the node-tree and the corresponding
                session.
            path_segments: A tuple describing the path.

        Returns:
            A node of the appropriate type.
        """
        contains_wildcards = any(segment == WILDCARD for segment in path_segments)
        if contains_wildcards:
            subtree_paths: (
                NestedDict[list[list[NormalizedPathSegment]] | dict]
                | UndefinedStructure
            ) = UndefinedStructure()
        else:
            subtree_paths = tree_manager.find_substructure(path_segments)

        is_leaf = not bool(subtree_paths)

        if contains_wildcards:
            return WildcardNode(
                tree_manager=tree_manager,
                path_segments=path_segments,
                subtree_paths=subtree_paths,
            )

        if is_leaf:
            return LeafNode(
                tree_manager=tree_manager,
                path_segments=path_segments,
                subtree_paths=subtree_paths,
            )

        return PartialNode(
            tree_manager=tree_manager,
            path_segments=path_segments,
            subtree_paths=subtree_paths,
        )

    @abstractmethod
    def try_generate_subnode(
        self,
        next_path_segment: NormalizedPathSegment,
    ) -> Node:
        """Provides nodes for the extended path or the original values for leafs.

        Args:
            next_path_segment: Segment, with which the current path should be extended.

        Returns:
            New node-object, representing the extended path.

        """
        ...

    @abstractmethod
    async def wait_for_state_change(
        self,
        value: int | NodeEnum,
        *,
        invert: bool = False,
    ) -> None:
        """Waits until the node has the expected state/value.

        Warning:
            Only supports integer and keyword nodes. (The value can either be the value
            or its corresponding enum value as string)

        Args:
            value: Expected value of the node.
            invert: Instead of waiting for the value, the function will wait for
                any value except the passed value. (default = False)
                Useful when waiting for value to change from existing one.
        """
        ...

    @t.overload
    async def subscribe(
        self,
        *,
        get_initial_value: bool = False,
        **kwargs,
    ) -> DataQueue: ...

    @t.overload
    async def subscribe(
        self,
        queue_type: type[QueueProtocol],
        *,
        get_initial_value: bool = False,
        **kwargs,
    ) -> QueueProtocol: ...

    @abstractmethod
    async def subscribe(
        self,
        queue_type: type[QueueProtocol] | None = None,
        *,
        get_initial_value: bool = False,
        **kwargs,
    ) -> QueueProtocol | DataQueue:
        """Subscribe to a node.

        Subscribing to a node will cause the server to send updates that happen
        to the node to the client. The updates are sent to the client automatically
        without any further interaction needed. Every update will be put into the
        queue, which can be used to receive the updates.

        Warning:
            Currently one can only subscribe to nodes that are leaf-nodes.

        Note:
            A node can be subscribed multiple times. Each subscription will
            create a new queue. The queues are independent of each other. It is
            however recommended to only subscribe to a node once and then fork
            the queue into multiple independent queues if needed. This will
            prevent unnecessary network traffic.

        Note:
            There is no explicit unsubscribe function. The subscription will
            automatically be cancelled when the queue is closed. This will
            happen when the queue is garbage collected or when the queue is
            closed manually.

        Args:
            queue_type: The type of the queue to be returned. This can be
                any class matching the DataQueue interface. Only needed if the
                default DataQueue class is not sufficient. If None is passed
                the default DataQueue class is used. (default=None)
            get_initial_value: If True, the initial value of the node is
                is placed in the queue. (default=False)
            kwargs: extra keyword arguments which are passed to the data-server
                to further configure the subscription.

        Returns:
            A DataQueue, which can be used to receive any changes to the node in a
            flexible manner.
        """

    @property
    def root(self) -> Node:
        """Providing root node.

        Depending on the hide_kelnel_prefix-setting of the
        NodeTreeManager, the root will either be '/' or
        the directly entered device, like '/dev1234'

        Returns:
            Root of the tree structure, this node is part of.
        """
        return self.tree_manager.root


class LeafNode(Node):
    """Node corresponding to a leaf in the path-structure."""

    async def _value_postprocessing(
        self,
        result: t.Awaitable[AnnotatedValue],
    ) -> AnnotatedValue:
        return self._tree_manager.parser(await result)

    def _get(self) -> t.Awaitable[AnnotatedValue]:
        """Get the value of the node.

        Returns:
            The current value of the node.

        Raises:
            OverwhelmedError: If the kernel is overwhelmed.
            BadRequestError: If the path is not readable.
            UnimplementedError: If the get request is not supported
                by the server.
            InternalError: If an unexpected internal error occurs.
            LabOneCoreError: If something else went wrong that can not be
                mapped to one of the other errors.
        """
        return self._value_postprocessing(self._tree_manager.session.get(self.path))

    def _set(
        self,
        value: Value,
    ) -> t.Awaitable[AnnotatedValue]:
        """Set the value of the node.

        Args:
            value: Value, which should be set to the node.

        Returns:
            The new value of the node.

        Raises:
            OverwhelmedError: If the kernel is overwhelmed.
            BadRequestError: If the path is not settable.
            UnimplementedError: If the set request is not
                supported by the server.
            InternalError: If an unexpected internal error occurs.
            LabOneCoreError: If something else went wrong that can not be
                mapped to one of the other errors.
        """
        return self._value_postprocessing(
            self._tree_manager.session.set(AnnotatedValue(value=value, path=self.path)),
        )

    def try_generate_subnode(
        self,
        next_path_segment: NormalizedPathSegment,
    ) -> Node:
        """Provides nodes for the extended path or the original values for leafs.

        Args:
            next_path_segment: Segment, with which the current path should be
                extended.

        Returns:
            New node-object, representing the extended path.

        Raises:
            LabOneInvalidPathError: Always, because extending leaf-paths is illegal.

        """
        msg = (
            f"Node '{self.path}' cannot be extended with "
            f"'/{next_path_segment}' because it is a leaf node."
        )
        raise LabOneInvalidPathError(msg)

    @t.overload
    async def subscribe(
        self,
        *,
        get_initial_value: bool = False,
        **kwargs,
    ) -> DataQueue: ...

    @t.overload
    async def subscribe(
        self,
        queue_type: type[QueueProtocol],
        *,
        get_initial_value: bool = False,
        **kwargs,
    ) -> QueueProtocol: ...

    async def subscribe(
        self,
        queue_type: type[QueueProtocol] | None = None,
        *,
        get_initial_value: bool = False,
        **kwargs,
    ) -> QueueProtocol | DataQueue:
        """Subscribe to a node.

        Subscribing to a node will cause the server to send updates that happen
        to the node to the client. The updates are sent to the client automatically
        without any further interaction needed. Every update will be put into the
        queue, which can be used to receive the updates.

        Note:
            A node can be subscribed multiple times. Each subscription will
            create a new queue. The queues are independent of each other. It is
            however recommended to only subscribe to a node once and then fork
            the queue into multiple independent queues if needed. This will
            prevent unnecessary network traffic.

        Note:
            There is no explicit unsubscribe function. The subscription will
            automatically be cancelled when the queue is closed. This will
            happen when the queue is garbage collected or when the queue is
            closed manually.

        Args:
            queue_type: The type of the queue to be returned. This can be
                any class matching the DataQueue interface. Only needed if the
                default DataQueue class is not sufficient. If None is passed
                the default DataQueue class is used. (default=None)

            get_initial_value: If True, the initial value of the node is
                is placed in the queue. (default=False)

            kwargs: extra keyword arguments which are passed to the data-server
                to further configure the subscription.

        Returns:
            A DataQueue, which can be used to receive any changes to the node in a
            flexible manner.
        """
        return await self._tree_manager.session.subscribe(
            self.path,
            parser_callback=self._tree_manager.parser,
            queue_type=queue_type or DataQueue,
            get_initial_value=get_initial_value,
            **kwargs,
        )

    async def wait_for_state_change(
        self,
        value: int | NodeEnum,
        *,
        invert: bool = False,
    ) -> None:
        """Waits until the node has the expected state/value.

        Warning:
            Only supports integer and keyword nodes. (The value can either be the value
            or its corresponding enum value as string)

        Args:
            value: Expected value of the node.
            invert: Instead of waiting for the value, the function will wait for
                any value except the passed value. (default = False)
                Useful when waiting for value to change from existing one.
        """
        await self._tree_manager.session.wait_for_state_change(
            self.path,
            value,
            invert=invert,
        )

    @cached_property
    def node_info(self) -> NodeInfo:
        """Additional information about the node."""
        return NodeInfo(self.tree_manager.path_to_info[self.path])


class WildcardOrPartialNode(Node, ABC):
    """Common functionality for wildcard and partial nodes."""

    def _get(
        self,
    ) -> t.Awaitable[ResultNode]:
        """Get the value of the node.

        Raises:
            OverwhelmedError: If the kernel is overwhelmed.
            BadRequestError: If the path is not readable.
            UnimplementedError: If the get with expression request is not
                supported by the server.
            InternalError: If an unexpected internal error occurs
            LabOneCoreError: If something else went wrong that can not be
                mapped to one of the other errors.
        """
        return self._package_response(
            self._tree_manager.session.get_with_expression(self.path),
        )

    def _set(
        self,
        value: Value,
    ) -> t.Awaitable[ResultNode]:
        """Set the value of the node.

        Args:
            value: Value, which should be set to the node.

        Raises:
            OverwhelmedError: If the kernel is overwhelmed.
            BadRequestError: If the path is not readable.
            UnimplementedError: If the get with expression request is not
                supported by the server.
            InternalError: If an unexpected internal error occurs
            LabOneCoreError: If something else went wrong that can not be
                mapped to one of the other errors.
        """
        return self._package_response(
            self._tree_manager.session.set_with_expression(
                AnnotatedValue(value=value, path=self.path),
            ),
        )

    async def _package_response(
        self,
        result: t.Awaitable[list[AnnotatedValue]],
    ) -> ResultNode:
        """Package server-response of wildcard or partial get-request.

        The result node will start to index from the root of the tree:

        >>> result_node = device.demods["*"].sample["*"].x()
        >>> result_node.demods[0].sample[0].x

        Of course, only the paths matching the wildcard/partial path
        will be available in the result node.

        Args:
            result: server-response to get (or set) request

        Returns:
            Node-structure, representing the results.
        """
        raw_response = await result
        timestamp = raw_response[0].timestamp if raw_response else None
        path_segments = (
            self.tree_manager.root.path_segments
        )  # same starting point as root

        # replace values by enum values and parse if applicable
        raw_response = [self._tree_manager.parser(r) for r in raw_response]

        # package into dict
        response_dict = {
            annotated_value.path: annotated_value for annotated_value in raw_response
        }

        subtree_paths = build_prefix_dict(
            [
                split_path(ann.path)[len(path_segments) :]  # suffix after root path
                for ann in raw_response
            ],
        )

        return ResultNode(
            tree_manager=self.tree_manager,
            path_segments=path_segments,
            subtree_paths=subtree_paths,  # type: ignore[arg-type]
            value_structure=response_dict,
            timestamp=timestamp if timestamp else 0,
        )

    @t.overload
    async def subscribe(
        self,
        *,
        get_initial_value: bool = False,
        **kwargs,
    ) -> DataQueue: ...

    @t.overload
    async def subscribe(
        self,
        queue_type: type[QueueProtocol],
        *,
        get_initial_value: bool = False,
        **kwargs,
    ) -> QueueProtocol: ...

    async def subscribe(
        self,
        queue_type: type[QueueProtocol] | None = None,  # noqa: ARG002
        *,
        get_initial_value: bool = False,  # noqa: ARG002
        **kwargs,  # noqa: ARG002
    ) -> QueueProtocol | DataQueue:
        """Subscribe to a node.

        Currently not supported for wildcard and partial nodes.

        Raises:
            NotImplementedError: Always.
        """
        msg = (
            "Subscribing to paths with wildcards "
            "or non-leaf paths is not supported. "
            "Subscribe to a leaf node instead "
            "and make sure to not use wildcards in the path."
        )
        raise LabOneNotImplementedError(msg)


class WildcardNode(WildcardOrPartialNode):
    """Node corresponding to a path containing one or more wildcards."""

    def __contains__(self, item: str | int | Node) -> bool:
        msg = (
            "Checking if a wildcard-node contains a subnode is not supported."
            "For checking if a path is contained in a node, make sure to not use"
            "wildcards in the path."
        )
        raise LabOneInappropriateNodeTypeError(msg)

    def __iter__(self) -> t.Iterator[Node]:
        msg = (
            "Iterating through a wildcard-node is not supported. "
            "For iterating through child nodes, make sure to not "
            "use wildcards in the path."
        )
        raise LabOneInappropriateNodeTypeError(msg)

    def __len__(self) -> int:
        msg = (
            "Getting the length of a wildcard-node is not supported."
            "For getting the length of a node, make sure to not "
            "use wildcards in the path."
        )
        raise LabOneInappropriateNodeTypeError(msg)

    def try_generate_subnode(
        self,
        next_path_segment: NormalizedPathSegment,
    ) -> Node:
        """Provides nodes for the extended path or the original values for leafs.

        Will never fail, because wildcard-paths are not checked to have valid matchings.

        Args:
            next_path_segment: Segment, with which the current path should be
                extended.

        Returns:
            New node-object, representing the extended path.

        """
        extended_path = (*self._path_segments, next_path_segment)
        return self._tree_manager.path_segments_to_node(extended_path)

    async def wait_for_state_change(
        self,
        value: int | NodeEnum,
        *,
        invert: bool = False,
    ) -> None:
        """Waits until all wildcard-associated nodes have the expected state/value.

        Warning:
            Only supports integer and keyword nodes. (The value can either be the value
            or its corresponding enum value as string)

        Args:
            value: Expected value of the node.
            invert: Instead of waiting for the value, the function will wait for
                any value except the passed value. (default = False)
                Useful when waiting for value to change from existing one.
        """
        # find paths corresponding to this wildcard-path and put them into nodes
        resolved_nodes = [
            self.tree_manager.raw_path_to_node(path)
            for path in await self.tree_manager.session.list_nodes(self.path)
        ]
        await asyncio.gather(
            *[
                node.wait_for_state_change(value, invert=invert)
                for node in resolved_nodes
            ],
        )


class PartialNode(WildcardOrPartialNode):
    """Node corresponding to a path, which has not reached a leaf yet."""

    def try_generate_subnode(
        self,
        next_path_segment: NormalizedPathSegment,
    ) -> Node:
        """Provides nodes for the extended path or the original values for leafs.

        Will fail if the resulting Path is ill-formed.

        Args:
            next_path_segment: Segment, with which the current path should be
            extended.

        Returns:
            New node-object, representing the extended path, or plain value,
            if came to a leaf-node.

        Raises:
            LabOneInvalidPathError: If the extension leads to an invalid path.

        """
        extended_path = (*self._path_segments, next_path_segment)

        # first try to extend the path. Will fail if the resulting path is invalid
        try:
            self.tree_manager.find_substructure(extended_path)
        except LabOneInvalidPathError:
            # wildcards are always legal
            if next_path_segment == WILDCARD:
                return self._tree_manager.path_segments_to_node(extended_path)
            raise

        return self._tree_manager.path_segments_to_node(extended_path)

    async def wait_for_state_change(
        self,
        value: int | NodeEnum,  # noqa: ARG002
        *,
        invert: bool = False,  # noqa: ARG002
    ) -> None:
        """Not applicable for partial-nodes.

        Args:
            value: Expected value of the node.
            invert: Instead of waiting for the value, the function will wait for
                any value except the passed value. (default = False)
                Useful when waiting for value to change from existing one.

        Raises:
            LabOneInappropriateNodeTypeError: Always, because partial nodes cannot be
            waited for.
        """
        msg = (
            "Cannot wait for a partial node to change its value. Consider waiting "
            "for a change of one or more leaf-nodes instead."
        )
        raise LabOneInappropriateNodeTypeError(msg)
