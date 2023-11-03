"""Pythonic node-tree.

This module enables working on a nodetree with the dot-operator:
>>> zi.debug.level
A Node object will return another node object at access. These nodes can be used
for getting and setting values using the __call__-operator.
>>> zi.debug.level()
>>> zi.debug.level(6)

"""
from __future__ import annotations

import asyncio
import re
import typing as t
import uuid
from abc import ABC, abstractmethod
from functools import cached_property

from deprecation import deprecated  # type: ignore[import-untyped]

if t.TYPE_CHECKING:
    from labone.core.session import NodeInfo as NodeInfoType  # pragma: no cover
    from labone.nodetree.enum import NodeEnum  # pragma: no cover


from labone.core.value import AnnotatedValue, Value
from labone.nodetree.errors import (
    LabOneInappropriateNodeTypeError,
    LabOneInvalidPathError,
)
from labone.nodetree.helper import (
    WILDCARD,
    NestedDict,
    NormalizedPathSegment,
    Session,
    TreeProp,
    UndefinedStructure,
    build_prefix_dict,
    get_prefix,
    join_path,
    normalize_path_segment,
    pythonify_path_segment,
    split_path,
)

if t.TYPE_CHECKING:
    from labone.core.helper import LabOneNodePath  # pragma: no cover
    from labone.core.session import NodeType  # pragma: no cover
    from labone.core.subscription import DataQueue  # pragma: no cover

T = t.TypeVar("T")


class OptionInfo(t.NamedTuple):
    """Representing structure of options in NodeInfo."""

    enum: str
    description: t.Any


class NodeInfo:
    """Encapsulating information about a node.

    Among other, this class can answer weather the corresponding node
    is readable, writable, which values can be set, how values coming from the server
    should be preprocessed (parsed) ...

    This node is initially gained from remote and can be used to handle the node
    properly.
    """

    def __init__(self, info: NodeInfoType, path: LabOneNodePath):
        self._path = path
        self._info: NodeInfoType = info

    def __getattr__(
        self,
        item: str,
    ) -> LabOneNodePath | str | NodeType | dict[str, str] | None:
        return self._info[item]  # type: ignore[literal-required]

    def __contains__(self, item: str) -> bool:
        return item in self.__dir__()

    def __hash__(self) -> int:
        return hash(self.path + "NodeInfo")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, NodeInfo):
            return False
        return self._info == other._info

    def __dir__(self) -> list[str]:
        return list(self._info.keys()) + [
            var
            for var, value in vars(self.__class__).items()
            if isinstance(value, property) and not var.startswith("_")
        ]

    def __repr__(self) -> str:
        return f'NodeInfo("{self._path}")'

    def __str__(self) -> str:
        string = self._path
        if "Description" in self._info:  # pragma: no cover
            string += "\n" + self._info["Description"]
        for key, value in self._info.items():
            if key == "Options":
                string += f"\n{key}:"
                for option, description in value.items():  # type: ignore[attr-defined]
                    string += f"\n    {option}: {description}"
            elif key not in ["Description", "Node", "SetParser", "GetParser"]:
                string += f"\n{key}: {value}"
        return string

    @property
    def readable(self) -> bool | None:
        """Flag if the node is readable."""
        return "Read" in self._info["Properties"]

    @property
    def writable(self) -> bool | None:
        """Flag if the node is writable."""
        return "Write" in self._info["Properties"]

    @property
    def is_setting(self) -> bool | None:
        """Flag if the node is a setting."""
        return "Setting" in self._info["Properties"]

    @property
    def is_vector(self) -> bool | None:
        """Flag if the value of the node a vector."""
        return "Vector" in self._info["Type"]

    @property
    def path(self) -> str:
        """Path (LabOne representation) of the node."""
        return self._path

    @property
    def description(self) -> str:
        """Description of the node."""
        return self._info["Description"]

    @property
    def type(self) -> str:  # noqa: A003
        """Type of the node."""
        return self._info["Type"]

    @property
    def unit(self) -> str:
        """Unit of the node."""
        return self._info["Unit"]

    @cached_property
    def options(self) -> dict[int, OptionInfo]:
        """Options of the node."""
        option_map = {}
        for key, value in self._info.get("Options", {}).items():
            # Find all the keywords. We use only the first one
            # since it should be unambiguous
            enum_re = re.findall(r'"([a-zA-Z0-9-_"]+)"', value)
            enum = enum_re[0] if enum_re else ""

            # The description is either what comes after
            # the colon and space, or the whole string.
            # This is the case for nameless options, when the
            # key is an integer (for example demods/x/order)
            desc = re.findall(r'(?:.+":\s)?(.+)$', value)[0]

            option_map[int(key)] = OptionInfo(enum, desc)
        return option_map


class NodeTreeManager:
    """Managing relation of one nodetree and one session.

    Can be used to find nodes by path, caching them along the way.
    """

    def __init__(
        self,
        *,
        session: Session,
        path_to_info: dict[LabOneNodePath, NodeInfoType],
        parser: t.Callable[[AnnotatedValue], AnnotatedValue],
    ):
        """Retrieves the tree-structure and info to the nodes from the server.

        Executes the initialization procedure in order to acquire knowledge about
        the node-structure.
        (Later on, it is always assumed, that this structure has remained constant.)

        Args:
            session:
                Session from which the nodetree-data is retrieved.
            path_to_info:
                Result of former server-call, representing structure of tree plus
                information about each node.
            parser:
                Function, which is used to parse incoming values.
                It may do this in a path-specific manner.
        """
        self._session = session
        self._remembered_nodes: dict[tuple[NormalizedPathSegment, ...], Node] = {}
        self.path_to_info = path_to_info
        self._paths = self.path_to_info.keys()
        paths_as_segments = [split_path(path) for path in self._paths]

        # type casting to allow assignment of more general type into the dict
        self._partially_explored_structure: NestedDict[
            list[list[NormalizedPathSegment]] | dict
        ] = build_prefix_dict(
            paths_as_segments,
        )  # type: ignore[assignment]
        self.structure_info = self.path_to_info

        self._parser = parser

        self._cache_path_segments_to_node: (dict)[
            tuple[int, tuple[NormalizedPathSegment, ...]],
            Node,
        ] = {}
        self._cache_find_substructure: (dict)[
            tuple[int, tuple[NormalizedPathSegment, ...]],
            NestedDict[list[list[NormalizedPathSegment]] | dict],
        ] = {}

    def construct_nodetree(
        self,
        *,
        hide_kernel_prefix: bool = True,
    ) -> Node:
        """Provides a nodetree to work with.

        Args:
            hide_kernel_prefix:
                Enter a trivial first path-segment automatically.
                E.g. having the result of this function in a variable `tree`
                `tree.debug.info` can be used instead of `tree.device1234.debug.info`.
                Setting this option makes working with the tree easier.

        Returns:
            Root-node of the tree.

        """
        has_common_prefix = len(self._partially_explored_structure.keys()) == 1

        if not hide_kernel_prefix or not has_common_prefix:
            return self.path_segments_to_node(())

        common_prefix = next(iter(self._partially_explored_structure.keys()))
        return self.path_segments_to_node((common_prefix,))

    def find_substructure(
        self,
        path_segments: tuple[NormalizedPathSegment, ...],
    ) -> NestedDict[list[list[NormalizedPathSegment]] | dict]:
        """Find children and explore structure lazily as needed.

        All exploration at one place, so that nothing is explored twice.

        Args:
            path_segments: Path, for which the children should be found.

        Returns:
            Structure of the children of the path.

        Raises:
            LabOneInvalidPathError: If path is invalid.
        """
        unique_value = (hash(self), path_segments)
        if unique_value in self._cache_find_substructure:
            return self._cache_find_substructure[unique_value]

        # base case
        if not path_segments:
            return self._partially_explored_structure

        # solving recursively, taking advantage of caching
        # makes usual indexing of lower nodes O(1)
        reference = self.find_substructure(path_segments[:-1])
        segment = path_segments[-1]

        if not reference:
            msg = f"Path {join_path(path_segments)} "
            f"is invalid, because {join_path(path_segments[:-1])} "
            "is already a leaf-node."
            raise LabOneInvalidPathError(msg)

        try:
            reference[segment]
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
                f"\nViable extensions would be {list(reference.keys())}"
            )
            raise LabOneInvalidPathError(msg) from e

        # explore structure deeper on demand
        # (and only once, as structure remains resolved via reference within
        # self._partially_explored_structure)
        if not isinstance(reference[segment], dict):  # pragma: no cover
            reference[segment] = build_prefix_dict(
                reference[segment],  # type: ignore[arg-type]
            )

        result: NestedDict[list[list[NormalizedPathSegment]] | dict] = (reference)[
            segment
        ]  # type: ignore[assignment]
        self._cache_find_substructure[unique_value] = result
        return result

    def raw_path_to_node(
        self,
        path: LabOneNodePath,
    ) -> Node:
        """Obtain nodes in a cached manner.

        Caches node-objects and enforces thereby a singleton pattern.
        Only one node-object per path.

        Args:
            path:
                Path, for which a node object
                should be provided.

        Returns:
            Node-object corresponding to the path.

        Raises:
            LabOneInvalidPathError:
                In no subtree_paths are given and the path is invalid.
        """
        return self.path_segments_to_node(tuple(split_path(path)))

    # @cache_flexible(lambda self_, path_segments: (hash(self_), path_segments))
    def path_segments_to_node(
        self,
        path_segments: tuple[NormalizedPathSegment, ...],
    ) -> Node:
        """Obtain nodes in a cached manner.

        Caches node-objects and enforces thereby a singleton pattern.
        Only one node-object per path.

        Args:
            path_segments:
                Segments describing the path, for which a node object
                should be provided.

        Returns:
            Node-object corresponding to the path.

        Raises:
            LabOneInvalidPathError:
                In no subtree_paths are given and the path is invalid.
        """
        unique_value = (hash(self), path_segments)
        if unique_value in self._cache_path_segments_to_node:
            return self._cache_path_segments_to_node[unique_value]

        result = Node.build(self, path_segments)
        self._cache_path_segments_to_node[unique_value] = result
        return result

    def __hash__(self) -> int:
        return hash(id(self))

    @property
    def paths(self) -> t.KeysView[LabOneNodePath]:
        """Provides list of paths of all leaf-nodes.

        Returns:
            Paths of all leaf-nodes
        """
        return self._paths

    @property
    def parser(self) -> t.Callable[[AnnotatedValue], AnnotatedValue]:
        """Providing the enum-handler.

        Returns:
            Enum-handler of this Manager.
        """
        return self._parser

    @property
    def session(self) -> Session:
        """Providing the session.

        Returns:
            Session of this Manager.
        """
        return self._session


class MetaNode(ABC):
    """Basic functionality for a tree of nodes.

    This class provides common behavior for the Node and ResultNode classes.
    """

    def __init__(
        self,
        *,
        tree_manager: NodeTreeManager,
        path_segments: tuple[NormalizedPathSegment, ...],
        subtree_paths: NestedDict[list[list[NormalizedPathSegment]] | dict]
        | UndefinedStructure,
        path_aliases: dict[
            tuple[NormalizedPathSegment, ...],
            tuple[NormalizedPathSegment, ...],
        ]
        | None = None,
    ):
        """Initializing MetaNode.

        Args:
            tree_manager:
                Interface managing the node-tree and the corresponding session.
            path_segments:
                A tuple describing the path.
            subtree_paths:
                Structure, defining which sub-nodes exist.
                May contain a Nested dictionary or a list of paths. If a list is passed,
                a prefix-to-suffix-dictionary will be created out of it.
            path_aliases:
                When creating sub-nodes, these aliases are used to redirect
                certain paths. This attribute is useful for creating artificial nodes
                outside the normal structure. It will not be used in most other cases.
        """
        path_aliases = path_aliases if path_aliases is not None else {}

        self._tree_manager = tree_manager
        self._path_segments = path_segments
        self._path_aliases = path_aliases
        self._subtree_structure = subtree_paths

    def _redirect(
        self,
        path_segments: tuple[NormalizedPathSegment, ...],
    ) -> tuple[NormalizedPathSegment, ...]:
        """Use path aliases to redirect."""
        while path_segments in self._path_aliases:
            path_segments = self._path_aliases[path_segments]
        return path_segments

    @abstractmethod
    def __getattr__(self, next_segment: str) -> MetaNode | AnnotatedValue:
        """Go deeper into the tree structure.

        Args:
            next_segment:
                Segment, with which the current path should be extended


        Returns:
            New node-object, representing the extended path, or plain value,
            if came to a leaf-node.

        Example:
            >>> node.demods  # where 'demods' is the next segment to enter

        """
        ...  # pragma: no cover

    @abstractmethod
    def __getitem__(self, path_extension: str | int) -> MetaNode | AnnotatedValue:
        """Go one or multiple levels deeper into the tree structure.

        This operator can deal with a number of different scenarios, including:
        - simple path extensions
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

        Example:
            All these implementations are equivalent:
            >>> node['mds/groups/0']
            >>> node['mds']['groups'][0]
            >>> node.mds.groups[0]

        """
        ...  # pragma: no cover

    def __iter__(self) -> t.Iterator[MetaNode | AnnotatedValue]:
        """Iterating through direct sub-nodes.

        The paths are traversed in a sorted manner, providing a clear order.
        This is particularly useful when iterating through numbered child nodes,
        such as /0, /1, ...
        or alphabetically sorted child nodes.

        Returns:
            Iterator through sub-nodes.

        """
        for segment in sorted(self._subtree_structure.keys()):
            yield self[segment]

    def __len__(self) -> int:
        """Number of direct sub-nodes."""
        return len(self._subtree_structure.keys())

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self!s})"

    def __str__(self) -> str:
        return self.path

    @property
    def tree_manager(self) -> NodeTreeManager:
        """Get interface managing the node-tree and the corresponding session.

        Returns:
            Interface managing the node-tree and the corresponding session.
        """
        return self._tree_manager

    @property
    def path(self) -> LabOneNodePath:
        """The path, this node corresponds to.

        Returns:
            The path, this node corresponds to.
        """
        return join_path(self._path_segments)

    @property
    def path_segments(self) -> tuple[NormalizedPathSegment, ...]:
        """The segments of the path, this node corresponds to.

        Returns:
            The segments of the path, this node corresponds to.
        """
        return self._path_segments

    @property
    @deprecated(details="use 'path_segments' instead.")
    def raw_tree(self) -> tuple[NormalizedPathSegment, ...]:
        """The segments of the path, this node corresponds to.

        Returns:
            The segments of the path, this node corresponds to.
        """
        return self.path_segments

    @property
    def path_aliases(
        self,
    ) -> dict[tuple[NormalizedPathSegment, ...], tuple[NormalizedPathSegment, ...]]:
        """Path aliases of this node.

        When creating sub-nodes, these aliases are used to redirect
        certain paths. This attribute is useful for creating artificial nodes
        outside the normal structure. It will not be used in most other cases.

        Returns:
            Path aliases of this node.
        """
        return self._path_aliases  # pragma: no cover

    @property
    def subtree_structure(
        self,
    ) -> NestedDict[list[list[NormalizedPathSegment]] | dict] | UndefinedStructure:
        """Structure defining which sub-nodes exist.

        Returns:
            Structure defining which sub-nodes exist.
        """
        return self._subtree_structure

    def is_child_node(
        self,
        child_node: MetaNode | t.Sequence[NormalizedPathSegment],
    ) -> bool:
        """Checks if a node is a direct child node of this node.

        Children of children (etc.) will not be counted as direct children.
        The node itself is also not counted as its child.

        Args:
            child_node:
                Potential child node.

        Returns:
            Boolean if passed node is a child node.
        """
        path_segments = (
            child_node.path_segments
            if isinstance(child_node, MetaNode)
            else tuple(child_node)
        )
        return (
            self.path_segments == path_segments[:-1]
            and path_segments[-1] in self._subtree_structure
        )


class ResultNode(MetaNode):
    """Representing values of a get-request in form of a tree.

    If hold values at its leafs.
    """

    def __init__(  # noqa: PLR0913
        self,
        tree_manager: NodeTreeManager,
        path_segments: tuple[NormalizedPathSegment, ...],
        subtree_paths: NestedDict[list[list[NormalizedPathSegment]] | dict],
        value_structure: TreeProp,
        timestamp: int,
        path_aliases: dict[
            tuple[NormalizedPathSegment, ...],
            tuple[NormalizedPathSegment, ...],
        ]
        | None = None,
    ):
        """Tree-structure representing the results of a former request.

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
            path_aliases:
                When creating sub-nodes, these aliases are used to redirect
                certain paths. This attribute is useful for creating artificial nodes
                outside the normal structure. It will not be used in most other cases.
        """
        super().__init__(
            tree_manager=tree_manager,
            path_segments=path_segments,
            subtree_paths=subtree_paths,
            path_aliases=path_aliases,
        )
        self._value_structure = value_structure
        self._timestamp = timestamp

    def __getattr__(self, next_segment: str) -> ResultNode | AnnotatedValue:
        """Go deeper into the tree structure.

        Args:
            next_segment:
                Segment, with which the current path should be extended.

        Returns:
            New node-object, representing the extended path, or plain value,
            if came to a leaf-node.

        Example:
            >>> node.demods  # where 'demods' is the next segment to enter

        """
        return self.try_generate_subnode(normalize_path_segment(next_segment))

    def __getitem__(self, path_extension: str | int) -> ResultNode | AnnotatedValue:
        """Go one or multiple levels deeper into the tree structure.

        This operator can deal with a number of different scenarios, including:
        - simple path extensions
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

        Example:
            All these implementations are equivalent:
            >>> node['mds/groups/0']
            >>> node['mds']['groups'][0]
            >>> node.mds.groups[0]

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

    def try_generate_subnode(
        self,
        next_path_segment: NormalizedPathSegment,
    ) -> ResultNode | AnnotatedValue:
        """Provides nodes for the extended path or the original values for leafs.

        Will fail if the resulting Path is ill-formed.

        Args:
            next_path_segment:
                Segment, with which the current path should be extended.

        Returns:
            New node-object, representing the extended path, or plain value,
            if came to a leaf-node.

        Raises:
            LabOneInvalidPathError: If the extension leads to an invalid path or if it
                is tried to use wildcards in ResultNodes.

        """
        extended_path = self._redirect((*self._path_segments, next_path_segment))

        try:
            next_subtree = self.tree_manager.find_substructure(extended_path)
        except LabOneInvalidPathError as e:
            if next_path_segment == WILDCARD:
                msg = (
                    f"Wildcards '*' are not allowed in a tree representing "
                    f"measurement results. However, it was tried to extend {self.path} "
                    f"with a wildcard."
                )
                raise LabOneInvalidPathError(msg) from e
            raise LabOneInvalidPathError from e

        deeper_node = ResultNode(
            tree_manager=self.tree_manager,
            path_segments=extended_path,
            subtree_paths=next_subtree,
            value_structure=self._value_structure,
            timestamp=self._timestamp,
            path_aliases=self._path_aliases,
        )

        if not next_subtree:
            # give value instead of subnode if already at a leaf
            # generate hypothetical node in order to apply normal behavior,
            # including path alias redirection
            return self._value_structure[deeper_node.path]

        return deeper_node

    def __dir__(self) -> t.Iterable[str]:
        """Show valid subtree-extensions in hints.

        Returns:
            Iterator of valid dot-access identifier.

        """
        return [pythonify_path_segment(p) for p in self._subtree_structure] + list(
            super().__dir__(),
        )

    def __contains__(self, item: str | int | ResultNode | AnnotatedValue) -> bool:
        """Checks if a path-segment or node is an immediate sub-node of this one."""
        if isinstance(item, ResultNode):
            return self.is_child_node(item)
        if isinstance(item, AnnotatedValue):
            return self.is_child_node(split_path(item.path))
        return normalize_path_segment(item) in self._subtree_structure

    def __call__(self, *_, **__) -> None:
        """Do not use. Showing an error to express result-nodes can't be get/set.

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
            path: self._value_structure[path].value for path in self._value_structure
        }
        return (
            f"{self.__class__.__name__}('{self.path}', time: #{self._timestamp}, "
            f"data: {value_dict})"
        )

    def __repr__(self) -> str:
        return f"{self!s} -> {[str(k) for k in self._subtree_structure]}"


class Node(MetaNode, ABC):
    """Nodes of (lazy) tree-structure, representing paths.

    Can be used to get and set values or subscribe to a path.
    """

    @classmethod
    def build(
        cls,
        tree_manager: NodeTreeManager,
        path_segments: tuple[NormalizedPathSegment, ...],
        path_aliases: dict[
            tuple[NormalizedPathSegment, ...],
            tuple[NormalizedPathSegment, ...],
        ]
        | None = None,
    ) -> Node:
        """Create appropriate type of node.

        Useful for creating a node, not necessarily knowing what kind of node it
        should be. This factory method will choose the correct one.

        Args:
            tree_manager:
                Interface managing the node-tree and the corresponding session.
            path_segments: A tuple describing the path.
            path_aliases:
                When creating sub-nodes, these aliases are used to redirect
                certain paths. This attribute is useful for creating artificial nodes
                outside the normal structure. It will not be used in most other cases.

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
                path_aliases=path_aliases,
            )

        if is_leaf:
            return LeafNode(
                tree_manager=tree_manager,
                path_segments=path_segments,
                subtree_paths=subtree_paths,
                path_aliases=path_aliases,
            )

        return PartialNode(
            tree_manager=tree_manager,
            path_segments=path_segments,
            subtree_paths=subtree_paths,
            path_aliases=path_aliases,
        )

    def __getattr__(self, next_segment: str) -> Node:
        """Go deeper into the tree structure.

        Args:
            next_segment:
                Segment, with which the current path should be extended


        Returns:
            New node-object, representing the extended path, or plain value,
            if came to a leaf-node.

        Example:
            >>> node.demods  # where 'demods' is the next segment to enter

        """
        return self.try_generate_subnode(normalize_path_segment(next_segment))

    def __getitem__(self, path_extension: str | int) -> Node:
        """Go one or multiple levels deeper into the tree structure.

        This operator can deal with a number of different scenarios, including:
        - simple path extensions
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

        Returns: New node-object, representing the extended path

        Example:
            All these implementations are equivalent:
            >>> node['mds/groups/0']
            >>> node['mds']['groups'][0]
            >>> node.mds.groups[0]

        """
        relative_path_segments = split_path(str(path_extension))
        current_node = self

        for path_segment in relative_path_segments:
            current_node = current_node.try_generate_subnode(
                normalize_path_segment(path_segment),
            )

        return current_node

    @abstractmethod
    def try_generate_subnode(
        self,
        next_path_segment: NormalizedPathSegment,
    ) -> Node:
        """Provides nodes for the extended path or the original values for leafs.

        Args:
            next_path_segment:
                Segment, with which the current path should be extended.

        Returns:
            New node-object, representing the extended path.

        """
        ...  # pragma: no cover

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
        return [pythonify_path_segment(p) for p in self._subtree_structure] + list(
            super().__dir__(),
        )

    def __contains__(self, item: str | int | Node) -> bool:
        """Checks if a path-segment or node is an immediate sub-node of this one.

        Args:
            item:
                To be checked this is among the child-nodes. Can be called with
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
        return normalize_path_segment(item) in self._subtree_structure

    async def __call__(
        self,
        value: Value | None = None,
    ) -> AnnotatedValue | ResultNode:
        """Call with or without a value for setting/getting the node.

        Args:
            value:
                optional value, which is set to the node. If it is omitted, a get
                request is triggered instead.

        Returns: The current value of the node is returned either way. If a set-request
            is triggered, the new value will be given back. In case of non-leaf nodes,
            a node-structure representing the results of all sub-paths is returned.

        Raises:
            LabOneCoreError: If the node value type is not supported.
            LabOneConnectionError: If there is a problem in the connection.
            errors.LabOneTimeoutError: If the operation timed out.
            errors.LabOneWriteOnlyError: If a read operation was attempted on a
                write-only node.
            errors.LabOneCoreError: If something else went wrong.

        """
        if value is None:
            return await self._get()

        return await self._set(value)

    @abstractmethod
    async def _get(
        self,
    ) -> AnnotatedValue | ResultNode:
        ...  # pragma: no cover

    @abstractmethod
    async def _set(
        self,
        value: Value,
    ) -> AnnotatedValue | ResultNode:
        ...  # pragma: no cover

    @abstractmethod
    async def wait_for_state_change(
        self,
        value: int | NodeEnum,
        *,
        invert: bool = False,
        timeout: float = 2,
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
            timeout: Maximum wait time in seconds. (default = 2)

        Raises:
            TimeoutError: Timeout exceeded.
        """
        ...  # pragma: no cover


class LeafNode(Node):
    """Node corresponding to a leaf in the path-structure."""

    def try_generate_subnode(
        self,
        next_path_segment: NormalizedPathSegment,
    ) -> Node:
        """Provides nodes for the extended path or the original values for leafs.

        Args:
            next_path_segment:
                Segment, with which the current path should be extended.

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

    async def _get(self) -> AnnotatedValue:
        """Get the value of the node.

        Raises:
             LabOneConnectionError: If there is a problem in the connection.
             errors.LabOneTimeoutError: If the operation timed out.
             errors.LabOneWriteOnlyError: If a read operation was attempted on a
                 write-only node.
             errors.LabOneCoreError: If something else went wrong.
        """
        return self._tree_manager.parser(
            await self._tree_manager.session.get(self.path),
        )

    async def _set(
        self,
        value: Value,
    ) -> AnnotatedValue:
        """Set the value of the node.

        Raises:
            LabOneCoreError: If the node value type is not supported.
            LabOneConnectionError: If there is a problem in the connection.
        """
        return self._tree_manager.parser(
            await self._tree_manager.session.set(
                AnnotatedValue(value=value, path=self.path),
            ),
        )

    async def subscribe(self) -> DataQueue:
        """Stay informed of any changes to this node.

        Args:
            enum: (similar as in __call__-operator)
            parse: (similar as in __call__-operator)

        Returns: A DataQueue, which can be used to receive any changes to the node in a
            flexible manner.

        """
        parser_callback = self._tree_manager.parser
        return await self._tree_manager.session.subscribe(self.path, parser_callback)

    async def wait_for_state_change(
        self,
        value: int | NodeEnum,
        *,
        invert: bool = False,
        timeout: float = 2,
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
            timeout: Maximum wait time in seconds. (default = 2)

        Raises:
            TimeoutError: Timeout exceeded.
        """
        # order important so that no update can happen unseen by the queue after
        # regarding the current state
        queue, initial_state = await asyncio.gather(self.subscribe(), self())

        # correct value right at the beginning
        if (value == initial_state.value) ^ invert:
            return

        await asyncio.wait_for(
            self._wait_for_state_change_loop(queue, value=value, invert=invert),
            timeout,
        )

    @staticmethod
    async def _wait_for_state_change_loop(
        queue: DataQueue,
        value: int | str | NodeEnum,
        *,
        invert: bool = False,
    ) -> None:
        while True:
            new_value: AnnotatedValue = await queue.get()
            if (value == new_value.value) ^ invert:  # pragma: no cover
                return

    @cached_property
    def node_info(self) -> NodeInfo:
        """Get further information regarding this node as a dedicated object.

        Returns:
            NodeInfo-object, containing further information regarding this node.
        """
        return NodeInfo(self.tree_manager.path_to_info[self.path], self.path)


class WildcardOrPartialNode(Node, ABC):
    """Common functionality for wildcard and partial nodes."""

    async def _get(
        self,
    ) -> ResultNode:
        """Get the value of the node.

        Raises:
            LabOneCoreError: If the node value type is not supported.
            LabOneConnectionError: If there is a problem in the connection.
        """
        return self._package_get_response(
            await self._tree_manager.session.get_with_expression(self.path),
        )

    async def _set(
        self,
        value: Value,
    ) -> ResultNode:
        """Set the value of the node.

        Raises:
            LabOneCoreError: If the node value type is not supported.
            LabOneConnectionError: If there is a problem in the connection.
        """
        return self._package_get_response(
            await self._tree_manager.session.set_with_expression(
                AnnotatedValue(value=value, path=self.path),
            ),
        )

    @abstractmethod
    def _package_get_response(
        self,
        raw_response: list[AnnotatedValue],
    ) -> ResultNode:
        """Package server-response to wildcard-get-request in a friendly way.

        Args:
            raw_response:
                server-response to get (or set) request

        Returns:
            Node-structure, representing the results.
        """
        ...  # pragma: no cover


class WildcardNode(WildcardOrPartialNode):
    """Node corresponding to a path containing one or more wildcards."""

    def try_generate_subnode(
        self,
        next_path_segment: NormalizedPathSegment,
    ) -> Node:
        """Provides nodes for the extended path or the original values for leafs.

        Will never fail, because wildcard-paths are not checked to have valid matchings.

        Args:
            next_path_segment:
                Segment, with which the current path should be extended.

        Returns:
            New node-object, representing the extended path.

        """
        extended_path = self._redirect((*self._path_segments, next_path_segment))
        return self._tree_manager.path_segments_to_node(extended_path)

    def _package_get_response(
        self,
        raw_response: list[AnnotatedValue],
    ) -> ResultNode:
        """Package server-response to wildcard-get-request in a friendly way.

        There may be multiple matches to a wildcard-path. The function identifies
        those and builds an auxiliary ResultNode, which can be indexed e.g. [0] and
        will redirect to a certain match. (In this context, a 'match' means one
        specific path-prefix, which fits to the given wildcard-path.)

        Args:
            raw_response:
                server-response to get (or set) request

        Returns:
            Node-structure, representing the results.
        """
        timestamp = raw_response[0].timestamp if raw_response else None

        # replace values by enum values and parse if applicable
        raw_response = [self._tree_manager.parser(r) for r in raw_response]

        # package into dict
        response_dict = {
            annotated_value.path: annotated_value for annotated_value in raw_response
        }

        # find out which prefixes the wildcard-path has matched to
        prefix_length = len(self._path_segments)
        prefixes = list(
            {get_prefix(a.path, prefix_length) for a in raw_response},
        )

        # guarantee sorted matches later on
        prefixes.sort()

        # build a combined tree structure
        nodes = [
            self._tree_manager.path_segments_to_node(tuple(split_path(prefix)))
            for prefix in prefixes
        ]
        structure = {str(i): node.subtree_structure for i, node in enumerate(nodes)}

        # define redirection to original paths
        match_segment = f"matches_{uuid.uuid4()}_id"
        path_aliases: dict[tuple[str, ...], tuple[str, ...]] = {
            (match_segment, str(i)): node.path_segments for i, node in enumerate(nodes)
        }

        # define an auxiliary node, which bundles the results, but is not part of
        # the normal node-tree-structure
        return ResultNode(
            tree_manager=self.tree_manager,
            path_segments=(match_segment,),
            subtree_paths=structure,  # type: ignore[arg-type]
            value_structure=response_dict,
            timestamp=timestamp if timestamp else 0,
            path_aliases=path_aliases,
        )

    async def wait_for_state_change(
        self,
        value: int | NodeEnum,
        *,
        invert: bool = False,
        timeout: float = 2,
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
            timeout: Maximum wait time in seconds. (default = 2)

        Raises:
            TimeoutError: Timeout exceeded.
        """
        # find paths corresponding to this wildcard-path and put them into nodes
        resolved_nodes = [
            self.tree_manager.raw_path_to_node(path)
            for path in await self.tree_manager.session.list_nodes(self.path)
        ]
        await asyncio.gather(
            *[
                node.wait_for_state_change(value, invert=invert, timeout=timeout)
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
            next_path_segment:
                Segment, with which the current path should be extended.

        Returns:
            New node-object, representing the extended path, or plain value,
            if came to a leaf-node.

        Raises:
            LabOneInvalidPathError: If the extension leads to an invalid path.

        """
        extended_path = self._redirect((*self._path_segments, next_path_segment))

        # first try to extend the path. Will fail if the resulting path is invalid
        try:
            self.tree_manager.find_substructure(extended_path)
        except LabOneInvalidPathError as e:
            # wildcards are always legal
            if next_path_segment == WILDCARD:
                return self._tree_manager.path_segments_to_node(extended_path)
            raise LabOneInvalidPathError from e

        return self._tree_manager.path_segments_to_node(extended_path)

    def _package_get_response(
        self,
        raw_response: list[AnnotatedValue],
    ) -> ResultNode:
        """Package server-response to wildcard-get-request in a friendly way.

        If a node is not a leaf, but partial, multiple paths and values will
        be returned. Instead of giving a list, they are grouped into a tree.

        Args:
            raw_response:
                server-response to get (or set) request

        Returns:
            Node-structure, representing the results.
        """
        timestamp = raw_response[0].timestamp if raw_response else None

        # replace values by enum values and parse if applicable
        raw_response = [self._tree_manager.parser(r) for r in raw_response]

        # package into dict
        response_dict = {
            annotated_value.path: annotated_value for annotated_value in raw_response
        }

        return ResultNode(
            tree_manager=self.tree_manager,
            path_segments=self.path_segments,
            subtree_paths=self._subtree_structure,  # type: ignore[arg-type]
            value_structure=response_dict,
            timestamp=timestamp if timestamp else 0,
        )

    async def wait_for_state_change(
        self,
        value: int | NodeEnum,  # noqa: ARG002
        *,
        invert: bool = False,  # noqa: ARG002
        timeout: float = 2,  # noqa: ARG002
    ) -> None:
        """Not applicable for partial-nodes.

        Args:
            value: Expected value of the node.
            invert: Instead of waiting for the value, the function will wait for
                any value except the passed value. (default = False)
                Useful when waiting for value to change from existing one.
            timeout: Maximum wait time in seconds. (default = 2)

        Raises:
            LabOneInappropriateNodeTypeError: Always, because partial nodes cannot be
            waited for.
        """
        msg = (
            "Cannot wait for a partial node to change its value. Consider waiting "
            "for a change of one or more leaf-nodes instead."
        )
        raise LabOneInappropriateNodeTypeError(msg)
