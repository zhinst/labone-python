"""Base Instrument Driver.

Natively works with all device types.
"""
from __future__ import annotations

from labone.nodetree.node import Node, PartialNode


class Instrument(PartialNode):
    """Generic driver for a Zurich Instrument device.

    Note: It is implicitly assumed that the device is not a leaf node and does
        not contain wildcards.

    Args:
        serial: Serial number of the device, e.g. 'dev1000'.
            The serial number can be found on the back panel of the instrument.
        model_node: Example node which serves as a model for setting the inherited
            node attributes.
    """

    def __init__(
        self,
        *,
        serial: str,
        model_node: Node,
    ):
        self._serial = serial
        super().__init__(
            tree_manager=model_node.tree_manager,
            path_segments=model_node.path_segments,
            subtree_paths=model_node.subtree_paths,
            path_aliases=model_node.path_aliases,
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.serial})"

    @property
    def serial(self) -> str:
        """Instrument specific serial."""
        return self._serial
