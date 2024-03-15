"""Tests the conversion from capnp to AnnotatedValue"""

from unittest.mock import patch

import labone.core.value as value_module
import numpy as np
import pytest
from labone.core.errors import LabOneCoreError

from .resources import value_capnp


class IllegalAnnotatedValue:
    class IllegalValue:
        def which(self):
            return "illegal"

    @property
    def value(self):
        return IllegalAnnotatedValue.IllegalValue()


def test_illegal_type():
    with pytest.raises(ValueError):
        value_module.AnnotatedValue.from_capnp(IllegalAnnotatedValue())


def test_void():
    input_dict = {
        "metadata": {"timestamp": 42, "path": "/non/of/your/business"},
        "value": {"none": {}},
    }
    msg = value_capnp.AnnotatedValue.new_message()
    msg.from_dict(input_dict)
    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    assert parsed_value.timestamp == input_dict["metadata"]["timestamp"]
    assert parsed_value.path == input_dict["metadata"]["path"]
    assert parsed_value.extra_header is None
    assert parsed_value.value is None


def test_trigger_sample():
    input_dict = {
        "metadata": {"timestamp": 42, "path": "/non/of/your/business"},
        "value": {
            "triggerSample": {
                "timestamp": 1,
                "sampleTick": 2,
                "trigger": 3,
                "missedTriggers": 4,
                "awgTrigger": 5,
                "dio": 6,
                "sequenceIndex": 7,
            },
        },
    }
    msg = value_capnp.AnnotatedValue.new_message()
    msg.from_dict(input_dict)
    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    assert parsed_value.timestamp == input_dict["metadata"]["timestamp"]
    assert parsed_value.path == input_dict["metadata"]["path"]
    assert parsed_value.extra_header is None
    assert (
        parsed_value.value.timestamp
        == input_dict["value"]["triggerSample"]["timestamp"]
    )
    assert (
        parsed_value.value.sample_tick
        == input_dict["value"]["triggerSample"]["sampleTick"]
    )
    assert parsed_value.value.trigger == input_dict["value"]["triggerSample"]["trigger"]
    assert (
        parsed_value.value.missed_triggers
        == input_dict["value"]["triggerSample"]["missedTriggers"]
    )
    assert (
        parsed_value.value.awg_trigger
        == input_dict["value"]["triggerSample"]["awgTrigger"]
    )
    assert parsed_value.value.dio == input_dict["value"]["triggerSample"]["dio"]
    assert (
        parsed_value.value.sequence_index
        == input_dict["value"]["triggerSample"]["sequenceIndex"]
    )


def test_cnt_sample():
    input_dict = {
        "metadata": {"timestamp": 42, "path": "/non/of/your/business"},
        "value": {
            "cntSample": {
                "timestamp": 1,
                "counter": 2,
                "trigger": 3,
            },
        },
    }
    msg = value_capnp.AnnotatedValue.new_message()
    msg.from_dict(input_dict)
    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    assert parsed_value.timestamp == input_dict["metadata"]["timestamp"]
    assert parsed_value.path == input_dict["metadata"]["path"]
    assert parsed_value.extra_header is None
    assert parsed_value.value.timestamp == input_dict["value"]["cntSample"]["timestamp"]
    assert parsed_value.value.counter == input_dict["value"]["cntSample"]["counter"]
    assert parsed_value.value.trigger == input_dict["value"]["cntSample"]["trigger"]


def test_streaming_error():
    input_dict = {
        "metadata": {"timestamp": 42, "path": "/non/of/your/business"},
        "value": {
            "streamingError": {
                "code": 1,
                "message": "Test message",
                "category": "zi:test",
                "kind": "timeout",
                "source": "",
            },
        },
    }
    msg = value_capnp.AnnotatedValue.new_message()
    msg.from_dict(input_dict)
    with pytest.raises(LabOneCoreError):
        value_module.AnnotatedValue.from_capnp(msg)


@pytest.mark.parametrize(
    ("type_name", "input_val", "output_val"),
    [
        ("int64", 42, 42),
        ("double", 42.0, 42.0),
        ("complex", {"real": 42, "imag": 66}, 42 + 66j),
        ("string", "42", "42"),
    ],
)
def test_generic_types(type_name, input_val, output_val):
    input_dict = {
        "metadata": {"timestamp": 42, "path": "/non/of/your/business"},
        "value": {type_name: input_val},
    }
    msg = value_capnp.AnnotatedValue.new_message()
    msg.from_dict(input_dict)
    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    assert parsed_value.timestamp == input_dict["metadata"]["timestamp"]
    assert parsed_value.path == input_dict["metadata"]["path"]
    assert parsed_value.extra_header is None
    assert parsed_value.value == output_val


def test_string_vector():
    input_dict = {
        "metadata": {"timestamp": 42, "path": "/non/of/your/business"},
        "value": {
            "vectorData": {
                "valueType": 7,
                "vectorElementType": 6,
                "extraHeaderInfo": 0,
                "data": b"Hello World",
            },
        },
    }
    msg = value_capnp.AnnotatedValue.new_message()
    msg.from_dict(input_dict)
    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    assert parsed_value.timestamp == input_dict["metadata"]["timestamp"]
    assert parsed_value.path == input_dict["metadata"]["path"]
    assert parsed_value.extra_header is None
    assert parsed_value.value == "Hello World"


def test_generic_vector():
    input_array = np.array([1, 2, 3, 4, 5, 6, 7, 8], dtype=np.uint32)
    input_dict = {
        "metadata": {"timestamp": 42, "path": "/non/of/your/business"},
        "value": {
            "vectorData": {
                "valueType": 67,
                "vectorElementType": 2,
                "extraHeaderInfo": 0,
                "data": input_array.tobytes(),
            },
        },
    }
    msg = value_capnp.AnnotatedValue.new_message()
    msg.from_dict(input_dict)
    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    assert parsed_value.timestamp == input_dict["metadata"]["timestamp"]
    assert parsed_value.path == input_dict["metadata"]["path"]
    assert parsed_value.extra_header is None
    assert np.array_equal(parsed_value.value, input_array)


@patch("labone.core.value.parse_shf_vector_data_struct", autospec=True)
def test_shf_vector(mock_method):
    input_dict = {
        "metadata": {"timestamp": 42, "path": "/non/of/your/business"},
        "value": {
            "vectorData": {
                "valueType": 69,
                "vectorElementType": 2,
                "extraHeaderInfo": 0,
                "data": b"",
            },
        },
    }
    msg = value_capnp.AnnotatedValue.new_message()
    msg.from_dict(input_dict)
    mock_method.return_value = "array", "extra_header"
    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    mock_method.assert_called_once()
    assert mock_method.call_args[0][0].to_dict() == input_dict["value"]["vectorData"]
    assert parsed_value.timestamp == input_dict["metadata"]["timestamp"]
    assert parsed_value.path == input_dict["metadata"]["path"]
    assert parsed_value.extra_header == "extra_header"
    assert parsed_value.value == "array"


@patch.object(
    value_module,
    "parse_shf_vector_data_struct",
    autospec=True,
)
def test_unknown_shf_vector(mock_method):
    input_array = np.linspace(0, 1, 200, dtype=np.uint32)
    input_dict = {
        "metadata": {"timestamp": 42, "path": "/non/of/your/business"},
        "value": {
            "vectorData": {
                "valueType": 69,
                "vectorElementType": 2,
                "extraHeaderInfo": 32,
                "data": input_array.tobytes(),
            },
        },
    }
    msg = value_capnp.AnnotatedValue.new_message()
    msg.from_dict(input_dict)
    mock_method.side_effect = ValueError("Unknown SHF vector type")
    with pytest.raises(ValueError):
        value_module.AnnotatedValue.from_capnp(msg)
    mock_method.assert_called_once()
    assert mock_method.call_args[0][0].to_dict() == input_dict["value"]["vectorData"]
