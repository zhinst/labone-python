"""Dynamically build a type system from a parsed schema.

This is a modified version of the code from capnp.pycapnp.pxi
"""
from __future__ import annotations

import logging
import typing as t

from capnp.lib.capnp import (
    _DynamicStructBuilder,
    _DynamicStructReader,
    _EnumModule,
    _InterfaceModule,
    _StructABCMeta,
    _StructModule,
)

if t.TYPE_CHECKING:
    from typing_extensions import Self

    from labone.core.reflection.parsed_wire_schema import (
        LoadedNode,
        Schema,
    )

logger = logging.getLogger(__name__)


def _build_one_type(name: str, schema: Schema) -> type:
    """Build a single type from a schema.

    Based on the SchemaParser code from capnp.pycapnp.pxi

    Args:
        name: The name of the type.
        schema: The schema of the type.

    Returns:
        Created capnp type.
    """
    proto = schema.get_proto()
    if proto.isStruct:
        local_module = _StructModule(schema.as_struct(), name)

        class Reader(_DynamicStructReader):
            """An abstract base class.  Readers are 'instances' of this class."""

            __metaclass__ = _StructABCMeta
            __slots__ = []  # type: ignore[var-annotated]
            _schema = local_module.schema

            def __new__(cls) -> Self:
                msg = "This is an abstract base class"
                raise TypeError(msg)

        class Builder(_DynamicStructBuilder):
            """An abstract base class.  Builders are 'instances' of this class."""

            __metaclass__ = _StructABCMeta
            __slots__ = []  # type: ignore[var-annotated]
            _schema = local_module.schema

            def __new__(cls) -> Self:
                msg = "This is an abstract base class"
                raise TypeError(msg)

        local_module.Reader = Reader
        local_module.Builder = Builder

        return local_module
    if proto.isConst:
        return schema.as_const_value()
    if proto.isInterface:
        return _InterfaceModule(schema.as_interface(), name)
    if proto.isEnum:
        return _EnumModule(schema.as_enum(), name)
    # This should really not happen
    msg = f"Cannot load {name} because its of an unsupported type"
    raise RuntimeError(msg)


def _build_types_from_node(
    *,
    full_schema: dict[int, LoadedNode],
    module: object,
    node_id: int,
) -> None:
    """Recursively create a type from a node.

    This function will create a new type corresponding to the node with id
    "node_id", and then will create a nested type for each node that is a
    children of that node.

    Args:
        full_schema: The full schema of the capnp file.
        module: The module where the new type should be added.
        node_id: The id of the node to create.
    """
    node = full_schema[node_id]
    if node.name == "":
        logger.debug("Skipping node %s because it has no name", node_id)
        return
    logger.debug("Loading %s into module %s", node.name, module)
    submodule = _build_one_type(node.name, node.schema)
    setattr(module, node.name, submodule)
    for subnode in node.schema.get_proto().nestedNodes:
        if subnode.id not in full_schema:
            # This can for example happen for constants. They are listed as nested
            # nodes, but they are not part of the full schema.
            logger.debug("%s not in full schema", subnode.id)
            continue
        # Call recursively to build subnodes
        _build_types_from_node(
            full_schema=full_schema,
            module=submodule,
            node_id=subnode.id,
        )


def _should_skip(node: LoadedNode, *, skip_files: list[str]) -> bool:
    """Check if a node should be skipped.

    This is used to skip nodes that are not needed in the type system.

    Args:
        node: The node to check.
        skip_files: List of files that should be skipped.

    Returns:
        Flag indicating if the node should be skipped.
    """
    if node.is_nested:
        return True
    # Given RPC "foo", the schema will have "foo$Params" and "foo$Results".
    # The client shouldn't need to access these, so we don't add them.
    if "$" in node.name:
        return True
    return any(skip_file in node.file_of_origin for skip_file in skip_files)


def build_type_system(
    full_schema: dict[int, LoadedNode],
    root_module: object,
    skip_files: list[str] | None = None,
) -> None:
    """Dynamically build a type system from a parsed schema.

    The type system is built in the root_module. The root_module is usually
    the module where the schema was loaded from.

    Note: there is some room for interpretation when building the full type
    hierarchy. Currently we flat it out excluding the file of origin. That means
    that if originally there was a car.capnp file containing a "Car" struct and
    a bike.capnp file containing a "Bike" struct, this will be available in
    root_module as root_module.Car and root_module.Bike. One could argue that it
    would be better to have root_module.car.Car and root_module.bike.Bike.

    Args:
        full_schema: The full schema of the capnp file.
        root_module: The module where the type system should be built.
        skip_files: List of files that should be skipped.
    """
    _skip_files = ["schema.capnp"] if skip_files is None else skip_files
    # The full schema is a flat dictionary indexed by node id. At the root
    # node we make only types that don't have a parents. Nodes with a parent
    # are built recursively once the parent is built.
    for node_id, loaded_node in full_schema.items():
        if _should_skip(loaded_node, skip_files=_skip_files):
            continue
        _build_types_from_node(
            full_schema=full_schema,
            module=root_module,
            node_id=node_id,
        )
