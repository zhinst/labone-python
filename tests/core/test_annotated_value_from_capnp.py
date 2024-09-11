"""Tests the conversion from capnp to AnnotatedValue"""

from unittest.mock import patch

import numpy as np
import pytest

import labone.core.value as value_module
from labone.core import errors, hpk_schema


def test_void():
    msg = hpk_schema.AnnotatedValue()
    msg.init_metadata(timestamp=42, path="/non/of/your/business")
    msg.init_value().init_none()

    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    assert parsed_value.timestamp == msg.metadata.timestamp
    assert parsed_value.path == msg.metadata.path
    assert parsed_value.value is None


def test_trigger_sample():
    msg = hpk_schema.AnnotatedValue()
    msg.init_metadata(timestamp=42, path="/non/of/your/business")
    trigger_sample = msg.init_value().init_triggerSample()
    trigger_sample.timestamp = 1
    trigger_sample.sampleTick = 2
    trigger_sample.trigger = 3
    trigger_sample.missedTriggers = 4
    trigger_sample.awgTrigger = 5
    trigger_sample.dio = 6
    trigger_sample.sequenceIndex = 7

    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    assert parsed_value.timestamp == msg.metadata.timestamp
    assert parsed_value.path == msg.metadata.path
    assert parsed_value.value.timestamp == trigger_sample.timestamp
    assert parsed_value.value.sampleTick == trigger_sample.sampleTick
    assert parsed_value.value.trigger == trigger_sample.trigger
    assert parsed_value.value.missedTriggers == trigger_sample.missedTriggers
    assert parsed_value.value.awgTrigger == trigger_sample.awgTrigger
    assert parsed_value.value.dio == trigger_sample.dio
    assert parsed_value.value.sequenceIndex == trigger_sample.sequenceIndex


def test_cnt_sample():
    msg = hpk_schema.AnnotatedValue()
    msg.init_metadata(timestamp=42, path="/non/of/your/business")
    cnt_sample = msg.init_value().init_cntSample()
    cnt_sample.timestamp = 1
    cnt_sample.counter = 2
    cnt_sample.trigger = 3
    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    assert parsed_value.timestamp == msg.metadata.timestamp
    assert parsed_value.path == msg.metadata.path
    assert parsed_value.value.timestamp == cnt_sample.timestamp
    assert parsed_value.value.counter == cnt_sample.counter
    assert parsed_value.value.trigger == cnt_sample.trigger


@pytest.mark.parametrize(
    ("kind", "error_class"),
    [
        ("ok", errors.LabOneCoreError),
        ("cancelled", errors.CancelledError),
        ("notFound", errors.NotFoundError),
        ("unknown", errors.LabOneCoreError),
        ("overwhelmed", errors.OverwhelmedError),
        ("badRequest", errors.BadRequestError),
        ("unimplemented", errors.UnimplementedError),
        ("internal", errors.InternalError),
        ("unavailable", errors.UnavailableError),
        ("timeout", errors.LabOneTimeoutError),
    ],
)
def test_streaming_error(kind, error_class):
    msg = hpk_schema.AnnotatedValue()
    msg.init_metadata(timestamp=42, path="/non/of/your/business")
    msg.init_value().init_streamingError()
    msg.value.streamingError.code = 1
    msg.value.streamingError.message = "Test message"
    msg.value.streamingError.category = "zi:test"
    msg.value.streamingError.kind = kind
    msg.value.streamingError.source = ""
    with pytest.raises(error_class):
        value_module.AnnotatedValue.from_capnp(msg)


@pytest.mark.parametrize(
    ("type_name", "input_val", "output_val"),
    [
        ("int64", 42, 42),
        ("double", 42.0, 42.0),
        ("complex", {"real": 42, "imag": 66}, 42 + 66j),
        ("complex", 42 + 66j, 42 + 66j),
        ("string", "42", "42"),
    ],
)
def test_generic_types(type_name, input_val, output_val):
    msg = hpk_schema.AnnotatedValue()
    msg.init_metadata(timestamp=42, path="/non/of/your/business")
    setattr(msg.init_value(), type_name, input_val)
    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    assert parsed_value.timestamp == msg.metadata.timestamp
    assert parsed_value.path == msg.metadata.path
    assert parsed_value.value == output_val


def test_string_vector():
    msg = hpk_schema.AnnotatedValue()
    msg.init_metadata(timestamp=42, path="/non/of/your/business")
    msg.init_value().init_vectorData()
    msg.value.vectorData.valueType = 7
    msg.value.vectorData.vectorElementType = 6
    msg.value.vectorData.extraHeaderInfo = 0
    msg.value.vectorData.data = b"Hello World"
    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    assert parsed_value.timestamp == msg.metadata.timestamp
    assert parsed_value.path == msg.metadata.path
    assert parsed_value.value == "Hello World"


def test_generic_vector():
    input_array = np.array([1, 2, 3, 4, 5, 6, 7, 8], dtype=np.uint32)
    msg = hpk_schema.AnnotatedValue()
    msg.init_metadata(timestamp=42, path="/non/of/your/business")
    msg.init_value().init_vectorData()
    msg.value.vectorData.valueType = 67
    msg.value.vectorData.vectorElementType = 2
    msg.value.vectorData.extraHeaderInfo = 0
    msg.value.vectorData.data = input_array.tobytes()
    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    assert parsed_value.timestamp == msg.metadata.timestamp
    assert parsed_value.path == msg.metadata.path
    assert np.array_equal(parsed_value.value, input_array)


@patch.object(
    value_module,
    "parse_shf_vector_from_vector_data",
    autospec=True,
)
def test_shf_vector(mock_method):
    msg = hpk_schema.AnnotatedValue()
    msg.init_metadata(timestamp=42, path="/non/of/your/business")
    msg.init_value().init_vectorData()
    msg.value.vectorData.valueType = 69
    msg.value.vectorData.vectorElementType = 2
    msg.value.vectorData.extraHeaderInfo = 0
    msg.value.vectorData.data = b""
    mock_method.return_value = "array", "extra_header"
    parsed_value = value_module.AnnotatedValue.from_capnp(msg)
    mock_method.assert_called_once()
    assert mock_method.call_args[0][0] == msg.value.vectorData
    assert parsed_value.timestamp == msg.metadata.timestamp
    assert parsed_value.path == msg.metadata.path
    assert parsed_value.value == ("array", "extra_header")


@patch.object(
    value_module,
    "parse_shf_vector_from_vector_data",
    autospec=True,
)
def test_unknown_shf_vector(mock_method):
    input_array = np.linspace(0, 1, 200, dtype=np.uint32)
    msg = hpk_schema.AnnotatedValue()
    msg.init_metadata(timestamp=42, path="/non/of/your/business")
    msg.init_value().init_vectorData()
    msg.value.vectorData.valueType = 69
    msg.value.vectorData.vectorElementType = 2
    msg.value.vectorData.extraHeaderInfo = 32
    msg.value.vectorData.data = input_array.tobytes()
    mock_method.side_effect = ValueError("Unknown SHF vector type")
    with pytest.raises(ValueError):
        value_module.AnnotatedValue.from_capnp(msg)
    mock_method.assert_called_once()
    assert mock_method.call_args[0][0] == msg.value.vectorData


def test_shf_demodulator_vector_data():
    input_array = np.linspace(0, 1, 200, dtype=np.float64)
    msg = hpk_schema.AnnotatedValue()
    msg.init_metadata(timestamp=42, path="/non/of/your/business")
    msg.init_value().init_shfDemodData()
    msg.value.shfDemodData.x = input_array
    msg.value.shfDemodData.y = input_array
    msg.value.shfDemodData.properties.timestamp = 42
    msg.value.shfDemodData.properties.centerFreq = 1e6
    result = value_module.AnnotatedValue.from_capnp(msg)
    assert result.timestamp == msg.metadata.timestamp
    assert result.path == msg.metadata.path
    assert np.allclose(result.value.x, input_array)
    assert np.allclose(result.value.y, input_array)
    assert result.value.properties.timestamp == 42
    assert result.value.properties.centerFreq == 1e6


def test_shf_result_logger_vector_data():
    input_array = np.linspace(0, 1, 200, dtype=np.float64)
    msg = hpk_schema.AnnotatedValue()
    msg.init_metadata(timestamp=42, path="/non/of/your/business")
    msg.init_value().init_shfResultLoggerData()
    msg.value.shfResultLoggerData.vector.real = input_array
    msg.value.shfResultLoggerData.properties.timestamp = 42
    msg.value.shfResultLoggerData.properties.centerFrequency = 1e6
    result = value_module.AnnotatedValue.from_capnp(msg)
    assert result.timestamp == msg.metadata.timestamp
    assert result.path == msg.metadata.path
    assert np.allclose(result.value.vector, input_array)
    assert result.value.properties.timestamp == 42
    assert result.value.properties.centerFrequency == 1e6


def test_complex_shf_result_logger_vector_data():
    input_array = np.linspace(0, 1, 200, dtype=np.complex128)
    msg = hpk_schema.AnnotatedValue()
    msg.init_metadata(timestamp=42, path="/non/of/your/business")
    msg.init_value().init_shfResultLoggerData()
    msg.value.shfResultLoggerData.vector.complex = input_array
    msg.value.shfResultLoggerData.properties.timestamp = 42
    msg.value.shfResultLoggerData.properties.centerFrequency = 1e6
    result = value_module.AnnotatedValue.from_capnp(msg)
    assert result.timestamp == msg.metadata.timestamp
    assert result.path == msg.metadata.path
    assert np.allclose(result.value.vector, input_array)
    assert result.value.properties.timestamp == 42
    assert result.value.properties.centerFrequency == 1e6


def test_shf_scope_vector_data():
    input_array = np.linspace(0, 1, 200, dtype=np.float64)
    msg = hpk_schema.AnnotatedValue()
    msg.init_metadata(timestamp=42, path="/non/of/your/business")
    msg.init_value().init_shfScopeData()
    msg.value.shfScopeData.vector.real = input_array
    msg.value.shfScopeData.properties.timestamp = 42
    msg.value.shfScopeData.properties.centerFrequency = 1e6
    result = value_module.AnnotatedValue.from_capnp(msg)
    assert result.timestamp == msg.metadata.timestamp
    assert result.path == msg.metadata.path
    assert np.allclose(result.value.vector, input_array)
    assert result.value.properties.timestamp == 42
    assert result.value.properties.centerFrequency == 1e6


def test_complex_shf_scope_vector_data():
    input_array = np.linspace(0, 1, 200, dtype=np.complex128)
    msg = hpk_schema.AnnotatedValue()
    msg.init_metadata(timestamp=42, path="/non/of/your/business")
    msg.init_value().init_shfScopeData()
    msg.value.shfScopeData.vector.complex = input_array
    msg.value.shfScopeData.properties.timestamp = 42
    msg.value.shfScopeData.properties.centerFrequency = 1e6
    result = value_module.AnnotatedValue.from_capnp(msg)
    assert result.timestamp == msg.metadata.timestamp
    assert result.path == msg.metadata.path
    assert np.allclose(result.value.vector, input_array)
    assert result.value.properties.timestamp == 42
    assert result.value.properties.centerFrequency == 1e6


def test_shf_pid_vector_data():
    input_array = np.linspace(0, 1, 200, dtype=np.float64)
    msg = hpk_schema.AnnotatedValue()
    msg.init_metadata(timestamp=42, path="/non/of/your/business")
    msg.init_value().init_shfPidData()
    msg.value.shfPidData.value = input_array
    msg.value.shfPidData.error = input_array
    msg.value.shfPidData.properties.timestamp = 42
    msg.value.shfPidData.properties.triggerTimestamp = 54
    result = value_module.AnnotatedValue.from_capnp(msg)
    assert result.timestamp == msg.metadata.timestamp
    assert result.path == msg.metadata.path
    assert np.allclose(result.value.value, input_array)
    assert np.allclose(result.value.error, input_array)
    assert result.value.properties.timestamp == 42
    assert result.value.properties.triggerTimestamp == 54
