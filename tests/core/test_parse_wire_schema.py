from unittest.mock import MagicMock, patch

from labone.core.reflection import parsed_wire_schema


@patch("labone.core.reflection.parsed_wire_schema.capnp", autospec=True)
def test_reflection_server(capnp):
    encoded_schema = MagicMock()
    schema_loader = MagicMock()
    capnp.SchemaLoader.return_value = schema_loader

    encoded_nodes = [MagicMock(), MagicMock(), MagicMock()]

    def iter_encoded_nodes():
        yield from encoded_nodes

    encoded_schema.__iter__.side_effect = iter_encoded_nodes

    nodes = [MagicMock(), MagicMock(), MagicMock()]
    schema_loader.load_dynamic.side_effect = nodes
    nodes[0].get_proto.return_value.displayName = "Test:a.b.c"
    nodes[0].get_proto.return_value.id = "id1"
    nodes[1].get_proto.return_value.displayName = "Test2:a.b.d"
    nodes[1].get_proto.return_value.id = "id2"
    nodes[1].get_proto.return_value.nestedNodes = [nodes[0].get_proto.return_value]
    nodes[2].get_proto.return_value.displayName = "Test3:a.b.2"
    nodes[2].get_proto.return_value.id = "id3"

    parsed_schema = parsed_wire_schema.ParsedWireSchema(encoded_schema)
    assert len(parsed_schema.full_schema) == 3
    assert parsed_schema.full_schema["id1"].name == "c"
    assert parsed_schema.full_schema["id2"].name == "d"
    assert parsed_schema.full_schema["id3"].name == "2"
    assert parsed_schema.full_schema["id1"].file_of_origin == "Test"
    assert parsed_schema.full_schema["id2"].file_of_origin == "Test2"
    assert parsed_schema.full_schema["id3"].file_of_origin == "Test3"
    assert parsed_schema.full_schema["id1"].is_nested is True
    assert parsed_schema.full_schema["id2"].is_nested is False
    assert parsed_schema.full_schema["id3"].is_nested is False
    assert parsed_schema.full_schema["id1"].schema == nodes[0]
    assert parsed_schema.full_schema["id2"].schema == nodes[1]
    assert parsed_schema.full_schema["id3"].schema == nodes[2]
