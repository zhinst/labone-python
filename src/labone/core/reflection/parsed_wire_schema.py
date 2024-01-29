"""This module contains the ParsedWireSchema class.

It is used to recursively load the encoded schema of a LabOne server.
"""

from __future__ import annotations

import contextlib
import logging
import typing as t
from dataclasses import dataclass

import capnp

if t.TYPE_CHECKING:
    from typing_extensions import TypeAlias

logger = logging.getLogger(__name__)

EncodedSchema: TypeAlias = capnp.lib.capnp._DynamicListReader  # noqa: SLF001
Schema: TypeAlias = capnp.lib.capnp._Schema  # noqa: SLF001


@dataclass
class LoadedNode:
    """Single node of the parsed schema."""

    name: str
    schema: Schema
    file_of_origin: str
    is_nested: bool = False


class ParsedWireSchema:
    """Internal representation of capnp schema.

    Args:
        encoded_schema: The encoded capnp schema.
    """

    def __init__(self, encoded_schema: EncodedSchema) -> None:
        # This can probably be removed since the SchemaLoader will copy the
        # encoded schema anyway ... For now we keep it to ensure nothing is
        # deleted by capnp.
        self._encoded_schema = encoded_schema
        self._loader = capnp.SchemaLoader()
        self._full_schema = self._load_encoded_schema(encoded_schema)

    def _load_encoded_schema(
        self,
        encoded_schema: EncodedSchema,
    ) -> dict[int, LoadedNode]:
        """Load the encoded schema.

        Iterate through all schema nodes and use the capnp schema loader to
        store the schema in a dict (id: schema)

        Args:
            encoded_schema: The encoded capnp schema.

        Returns:
            The parsed schema represented as a dict (id: schema).
        """
        nodes: dict[int, LoadedNode] = {}
        nested_nodes = set()
        for serialized_node in encoded_schema:
            node = self._loader.load_dynamic(serialized_node)
            node_proto = node.get_proto()
            splitted_name = node_proto.displayName.split(":")
            full_name = splitted_name[1]
            name = full_name if "." not in full_name else full_name.split(".")[-1]
            loaded_node = LoadedNode(
                name=name,
                file_of_origin=splitted_name[0],
                schema=node,
            )
            nodes[node_proto.id] = loaded_node
            logging.debug("%s => %s", serialized_node.id, loaded_node.name)
            # Collect all nested nodes
            for nested_node in node_proto.nestedNodes:
                nested_nodes.add(nested_node.id)
        # Mark all nested nodes
        for node_id in nested_nodes:
            with contextlib.suppress(KeyError):
                nodes[node_id].is_nested = True
        return nodes

    @property
    def full_schema(self) -> dict[int, LoadedNode]:
        """The full schema of the server."""
        return self._full_schema
