"""Main entry point for the node tree package.

This module provides a function to construct a node tree from a session.
"""

from __future__ import annotations

import typing as t

from labone.nodetree.enum import get_default_enum_parser
from labone.nodetree.node import NodeTreeManager

if t.TYPE_CHECKING:
    from labone.core import AnnotatedValue
    from labone.nodetree.helper import Session
    from labone.nodetree.node import Node


async def construct_nodetree(
    session: Session,
    *,
    hide_kernel_prefix: bool = True,
    use_enum_parser: bool = True,
    custom_parser: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
) -> Node:
    """Create a nodetree structure from a LabOne session.

    Issues a single requests to the server to retrieve the structural
    information about the tree.

    Args:
        session: Connection to data-server.
        hide_kernel_prefix: Enter a trivial first path-segment automatically.
            E.g. having the result of this function in a variable `tree`
            `tree.debug.info` can be used instead of `tree.device1234.debug.info`.
            Setting this option makes working with the tree easier.
        use_enum_parser: Whether enumerated integer values coming from the server
            should be packaged into enum values, if applicable.
        custom_parser: A function that takes an annotated value and returns an
            annotated value. This function is applied to all values coming from
            the server. It is applied after the default enum parser, if
            applicable.

    Returns:
        Root-node of the tree.
    """
    path_to_info = await session.list_nodes_info("*")

    parser = get_default_enum_parser(path_to_info) if use_enum_parser else lambda x: x

    if custom_parser is not None:
        first_parser = parser  # this assignment prevents infinite recursion
        parser = lambda x: custom_parser(first_parser(x))  # type: ignore[misc] # noqa: E731

    nodetree_manager = NodeTreeManager(
        session=session,
        parser=parser,
        path_to_info=path_to_info,
    )

    return nodetree_manager.construct_nodetree(
        hide_kernel_prefix=hide_kernel_prefix,
    )
