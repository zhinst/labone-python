from typing import Any
from unittest.mock import MagicMock

import pytest
from capnp.lib.capnp import (
    _EnumModule,
    _InterfaceModule,
    _StructModule,
)
from labone.core.reflection import capnp_dynamic_type_system
from labone.core.reflection.parsed_wire_schema import LoadedNode


class DummyServer:
    def __init__(self):
        self._assigned_attr = {}

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            return super().__setattr__(name, value)
        self._assigned_attr[name] = value
        return None

    def assigned_attrs(self) -> dict:
        return self._assigned_attr


def test_reflection_type_empty_name():
    full_schema = {
        "id1": LoadedNode(
            name="",
            schema=MagicMock(),
            file_of_origin="Test1",
            is_nested=False,
        ),
    }
    root_module = DummyServer()

    capnp_dynamic_type_system.build_type_system(full_schema, root_module)
    assert not root_module.assigned_attrs()


def test_reflection_type_nested():
    full_schema = {
        "id1": LoadedNode(
            name="a",
            schema=MagicMock(),
            file_of_origin="Test1",
            is_nested=True,
        ),
    }
    root_module = DummyServer()

    capnp_dynamic_type_system.build_type_system(full_schema, root_module)
    assert not root_module.assigned_attrs()


def test_reflection_type_ignored_dollar_sign():
    full_schema = {
        "id1": LoadedNode(
            name="a$2",
            schema=MagicMock(),
            file_of_origin="Test1",
            is_nested=False,
        ),
        "id2": LoadedNode(
            name="$a",
            schema=MagicMock(),
            file_of_origin="Test1",
            is_nested=False,
        ),
        "id3": LoadedNode(
            name="a$",
            schema=MagicMock(),
            file_of_origin="Test1",
            is_nested=False,
        ),
    }
    root_module = DummyServer()

    capnp_dynamic_type_system.build_type_system(full_schema, root_module)
    assert not root_module.assigned_attrs()


def test_reflection_type_ignored_skip_name():
    full_schema = {
        "id1": LoadedNode(
            name="Hello/1/2/3",
            schema=MagicMock(),
            file_of_origin="Test1",
            is_nested=True,
        ),
        "id2": LoadedNode(
            name="/1/2/Hello",
            schema=MagicMock(),
            file_of_origin="Test1",
            is_nested=True,
        ),
        "id3": LoadedNode(
            name="Hello",
            schema=MagicMock(),
            file_of_origin="Test1",
            is_nested=True,
        ),
    }
    root_module = DummyServer()

    capnp_dynamic_type_system.build_type_system(
        full_schema,
        root_module,
        skip_files=["Hello"],
    )
    assert not root_module.assigned_attrs()


def test_reflection_type_struct():
    full_schema = {
        "id1": LoadedNode(
            name="a",
            schema=MagicMock(),
            file_of_origin="Test1",
            is_nested=False,
        ),
    }
    full_schema["id1"].schema.get_proto.return_value.isStruct = True
    full_schema["id1"].schema.get_proto.return_value.isConst = False
    full_schema["id1"].schema.get_proto.return_value.isInterface = False
    full_schema["id1"].schema.get_proto.return_value.isEnum = False
    root_module = DummyServer()

    capnp_dynamic_type_system.build_type_system(full_schema, root_module)
    assigned_attrs = root_module.assigned_attrs()
    assert len(assigned_attrs) == 1
    assert isinstance(assigned_attrs["a"], _StructModule)
    with pytest.raises(TypeError):
        assigned_attrs["a"].Reader()
    with pytest.raises(TypeError):
        assigned_attrs["a"].Builder()


def test_reflection_type_const():
    full_schema = {
        "id1": LoadedNode(
            name="a",
            schema=MagicMock(),
            file_of_origin="Test1",
            is_nested=False,
        ),
    }
    full_schema["id1"].schema.get_proto.return_value.isStruct = False
    full_schema["id1"].schema.get_proto.return_value.isConst = True
    full_schema["id1"].schema.get_proto.return_value.isInterface = False
    full_schema["id1"].schema.get_proto.return_value.isEnum = False

    root_module = DummyServer()

    capnp_dynamic_type_system.build_type_system(full_schema, root_module)
    assigned_attrs = root_module.assigned_attrs()
    assert len(assigned_attrs) == 1
    assert assigned_attrs["a"] == full_schema["id1"].schema.as_const_value()


def test_reflection_type_interface():
    full_schema = {
        "id1": LoadedNode(
            name="a",
            schema=MagicMock(),
            file_of_origin="Test1",
            is_nested=False,
        ),
    }
    full_schema["id1"].schema.get_proto.return_value.isStruct = False
    full_schema["id1"].schema.get_proto.return_value.isConst = False
    full_schema["id1"].schema.get_proto.return_value.isInterface = True
    full_schema["id1"].schema.get_proto.return_value.isEnum = False
    root_module = DummyServer()

    capnp_dynamic_type_system.build_type_system(full_schema, root_module)
    assigned_attrs = root_module.assigned_attrs()
    assert len(assigned_attrs) == 1
    assert isinstance(assigned_attrs["a"], _InterfaceModule)


def test_reflection_type_enum():
    full_schema = {
        "id1": LoadedNode(
            name="a",
            schema=MagicMock(),
            file_of_origin="Test1",
            is_nested=False,
        ),
    }
    full_schema["id1"].schema.get_proto.return_value.isStruct = False
    full_schema["id1"].schema.get_proto.return_value.isConst = False
    full_schema["id1"].schema.get_proto.return_value.isInterface = False
    full_schema["id1"].schema.get_proto.return_value.isEnum = True
    root_module = DummyServer()

    capnp_dynamic_type_system.build_type_system(full_schema, root_module)
    assigned_attrs = root_module.assigned_attrs()
    assert len(assigned_attrs) == 1
    assert isinstance(assigned_attrs["a"], _EnumModule)


def test_reflection_type_unknown():
    full_schema = {
        "id1": LoadedNode(
            name="a",
            schema=MagicMock(),
            file_of_origin="Test1",
            is_nested=False,
        ),
    }
    full_schema["id1"].schema.get_proto.return_value.isStruct = False
    full_schema["id1"].schema.get_proto.return_value.isConst = False
    full_schema["id1"].schema.get_proto.return_value.isInterface = False
    full_schema["id1"].schema.get_proto.return_value.isEnum = False
    root_module = DummyServer()

    with pytest.raises(RuntimeError):
        capnp_dynamic_type_system.build_type_system(full_schema, root_module)


def test_reflection_type_creation_nested():
    full_schema = {
        "id1": LoadedNode(
            name="a",
            schema=MagicMock(),
            file_of_origin="Test1",
            is_nested=False,
        ),
        "id2": LoadedNode(
            name="b",
            schema=MagicMock(),
            file_of_origin="Test2",
            is_nested=True,
        ),
    }
    full_schema["id1"].schema.get_proto.return_value.isStruct = False
    full_schema["id1"].schema.get_proto.return_value.isConst = False
    full_schema["id1"].schema.get_proto.return_value.isInterface = True
    full_schema["id1"].schema.get_proto.return_value.isEnum = False
    nested_node = MagicMock()
    full_schema["id1"].schema.get_proto.return_value.nestedNodes = [nested_node]
    nested_node.id = "id2"

    full_schema["id2"].schema.get_proto.return_value.isStruct = False
    full_schema["id2"].schema.get_proto.return_value.isConst = False
    full_schema["id2"].schema.get_proto.return_value.isInterface = True
    full_schema["id2"].schema.get_proto.return_value.isEnum = False
    root_module = DummyServer()

    capnp_dynamic_type_system.build_type_system(full_schema, root_module)
    assigned_attrs = root_module.assigned_attrs()
    assert len(assigned_attrs) == 1
    assert isinstance(assigned_attrs["a"], _InterfaceModule)
    assert hasattr(assigned_attrs["a"], "b")
    assert isinstance(assigned_attrs["a"].b, _InterfaceModule)


def test_reflection_type_creation_nested_unknown():
    full_schema = {
        "id1": LoadedNode(
            name="a",
            schema=MagicMock(),
            file_of_origin="Test1",
            is_nested=False,
        ),
    }
    full_schema["id1"].schema.get_proto.return_value.isStruct = False
    full_schema["id1"].schema.get_proto.return_value.isConst = False
    full_schema["id1"].schema.get_proto.return_value.isInterface = True
    full_schema["id1"].schema.get_proto.return_value.isEnum = False
    nested_node = MagicMock()
    full_schema["id1"].schema.get_proto.return_value.nestedNodes = [nested_node]
    nested_node.id = "id2"
    root_module = DummyServer()

    capnp_dynamic_type_system.build_type_system(full_schema, root_module)
    assigned_attrs = root_module.assigned_attrs()
    assert len(assigned_attrs) == 1
    assert isinstance(assigned_attrs["a"], _InterfaceModule)
    assert not hasattr(assigned_attrs["a"], "b")
