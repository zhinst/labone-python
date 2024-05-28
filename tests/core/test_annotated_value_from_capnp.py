"""Tests the conversion from capnp to AnnotatedValue"""

from unittest.mock import patch

import numpy as np
import pytest
from munch import Munch

import labone.core.value as value_module
from labone.core.errors import LabOneCoreError


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
    msg = Munch.fromDict(
        {
            "metadata": {"timestamp": 42, "path": "/non/of/your/business"},
            "value": {"none": {}},
        },
    )
    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    assert parsed_value.timestamp == msg["metadata"]["timestamp"]
    assert parsed_value.path == msg["metadata"]["path"]
    assert parsed_value.extra_header is None
    assert parsed_value.value is None


def test_trigger_sample():
    msg = Munch.fromDict(
        {
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
        },
    )
    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    assert parsed_value.timestamp == msg["metadata"]["timestamp"]
    assert parsed_value.path == msg["metadata"]["path"]
    assert parsed_value.extra_header is None
    assert parsed_value.value.timestamp == msg["value"]["triggerSample"]["timestamp"]
    assert parsed_value.value.sample_tick == msg["value"]["triggerSample"]["sampleTick"]
    assert parsed_value.value.trigger == msg["value"]["triggerSample"]["trigger"]
    assert (
        parsed_value.value.missed_triggers
        == msg["value"]["triggerSample"]["missedTriggers"]
    )
    assert parsed_value.value.awg_trigger == msg["value"]["triggerSample"]["awgTrigger"]
    assert parsed_value.value.dio == msg["value"]["triggerSample"]["dio"]
    assert (
        parsed_value.value.sequence_index
        == msg["value"]["triggerSample"]["sequenceIndex"]
    )


def test_cnt_sample():
    msg = Munch.fromDict(
        {
            "metadata": {"timestamp": 42, "path": "/non/of/your/business"},
            "value": {
                "cntSample": {
                    "timestamp": 1,
                    "counter": 2,
                    "trigger": 3,
                },
            },
        },
    )
    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    assert parsed_value.timestamp == msg["metadata"]["timestamp"]
    assert parsed_value.path == msg["metadata"]["path"]
    assert parsed_value.extra_header is None
    assert parsed_value.value.timestamp == msg["value"]["cntSample"]["timestamp"]
    assert parsed_value.value.counter == msg["value"]["cntSample"]["counter"]
    assert parsed_value.value.trigger == msg["value"]["cntSample"]["trigger"]


def test_streaming_error():
    msg = Munch.fromDict(
        {
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
        },
    )
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
    msg = Munch.fromDict(
        {
            "metadata": {"timestamp": 42, "path": "/non/of/your/business"},
            "value": {type_name: input_val},
        },
    )
    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    assert parsed_value.timestamp == msg["metadata"]["timestamp"]
    assert parsed_value.path == msg["metadata"]["path"]
    assert parsed_value.extra_header is None
    assert parsed_value.value == output_val


def test_string_vector():
    msg = Munch.fromDict(
        {
            "metadata": {"timestamp": 42, "path": "/non/of/your/business"},
            "value": {
                "vectorData": {
                    "valueType": 7,
                    "vectorElementType": 6,
                    "extraHeaderInfo": 0,
                    "data": b"Hello World",
                },
            },
        },
    )
    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    assert parsed_value.timestamp == msg["metadata"]["timestamp"]
    assert parsed_value.path == msg["metadata"]["path"]
    assert parsed_value.extra_header is None
    assert parsed_value.value == "Hello World"


def test_generic_vector():
    input_array = np.array([1, 2, 3, 4, 5, 6, 7, 8], dtype=np.uint32)
    msg = Munch.fromDict(
        {
            "metadata": {"timestamp": 42, "path": "/non/of/your/business"},
            "value": {
                "vectorData": {
                    "valueType": 67,
                    "vectorElementType": 2,
                    "extraHeaderInfo": 0,
                    "data": input_array.tobytes(),
                },
            },
        },
    )
    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    assert parsed_value.timestamp == msg["metadata"]["timestamp"]
    assert parsed_value.path == msg["metadata"]["path"]
    assert parsed_value.extra_header is None
    assert np.array_equal(parsed_value.value, input_array)


@patch("labone.core.value.parse_shf_vector_data_struct", autospec=True)
def test_shf_vector(mock_method):
    msg = Munch.fromDict(
        {
            "metadata": {"timestamp": 42, "path": "/non/of/your/business"},
            "value": {
                "vectorData": {
                    "valueType": 69,
                    "vectorElementType": 2,
                    "extraHeaderInfo": 0,
                    "data": b"",
                },
            },
        },
    )
    mock_method.return_value = "array", "extra_header"
    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    mock_method.assert_called_once()
    assert mock_method.call_args[0][0] == msg["value"]["vectorData"]
    assert parsed_value.timestamp == msg["metadata"]["timestamp"]
    assert parsed_value.path == msg["metadata"]["path"]
    assert parsed_value.extra_header == "extra_header"
    assert parsed_value.value == "array"


@patch.object(
    value_module,
    "parse_shf_vector_data_struct",
    autospec=True,
)
def test_unknown_shf_vector(mock_method):
    input_array = np.linspace(0, 1, 200, dtype=np.uint32)
    msg = Munch.fromDict(
        {
            "metadata": {"timestamp": 42, "path": "/non/of/your/business"},
            "value": {
                "vectorData": {
                    "valueType": 69,
                    "vectorElementType": 2,
                    "extraHeaderInfo": 32,
                    "data": input_array.tobytes(),
                },
            },
        },
    )
    mock_method.side_effect = ValueError("Unknown SHF vector type")
    with pytest.raises(ValueError):
        value_module.AnnotatedValue.from_capnp(msg)
    mock_method.assert_called_once()
    assert mock_method.call_args[0][0] == msg["value"]["vectorData"]
