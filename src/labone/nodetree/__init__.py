"""Subpackage providing the pythonic node-tree.

This subpackage delivers tree-like classes to navigate node-paths efficiently.
These nodes allow communication to server to get or set values accordingly.

This module enables accessing LabOne nodes with the dot-operator:

>>> zi.debug.level

Internally LabOne used a path-based representation of the nodes
(e.g. /zi/debug/level). This module provides a mapping between
the path-based representation and the object-based representation.

A Node object will be created on the fly and no initial generation is done upfront.
This allows an fast and intuitive access to the nodes.

To get or set one or multiple values the call operator can be used on any node
object. Calling a node with no arguments will result in getting the values,
while passing a value will result in setting it.

>>> await zi.debug.level()
>>> await zi.debug.level(6)
"""

from labone.nodetree.entry_point import construct_nodetree

__all__ = ["construct_nodetree"]
