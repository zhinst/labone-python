"""Tests for `AnnotatedValue` and its' parts.

NOTE: `capnp` builder instances cannot be asserter for equality.

TODO: Tests for invalid Python value cases.
"""

import numpy as np
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from hypothesis.extra.numpy import arrays

from labone.core.helper import VectorElementType
from labone.core.shf_vector_data import (
    VectorValueType,
    preprocess_complex_shf_waveform_vector,
)
from labone.core.value import value_from_python_types


@given(st.integers(min_value=-np.int64(), max_value=np.int64()))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_int64(inp):
    value = value_from_python_types(inp, path="")
    assert value["int64"] == inp


@pytest.mark.parametrize(("inp", "out"), [(False, 0), (True, 1)])
def test_value_from_python_types_bool_to_int64(inp, out):
    value = value_from_python_types(inp, path="")
    assert value["int64"] == out


@given(st.floats(allow_nan=False))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_double(inp):
    value = value_from_python_types(inp, path="")
    assert value["double"] == inp


def test_value_from_python_types_np_nan():
    value1 = value_from_python_types(np.nan, path="")
    assert np.isnan(value1["double"])

    inp = complex(real=0.0, imag=np.nan.imag)
    value2 = value_from_python_types(inp, path="")
    assert value2["complex"]["real"] == inp.real
    assert value2["complex"]["imag"] == inp.imag


@given(st.complex_numbers(allow_nan=False))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_complex(inp):
    value = value_from_python_types(inp, path="")
    assert value["complex"]["real"] == inp.real
    assert value["complex"]["imag"] == inp.imag


@given(st.text())
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_string(inp):
    value = value_from_python_types(inp, path="")
    assert value["string"] == inp


@given(st.binary())
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_bytes(inp):
    value = value_from_python_types(inp, path="")
    vec_data = value["vectorData"]
    assert vec_data["valueType"] == VectorValueType.BYTE_ARRAY.value
    assert vec_data["extraHeaderInfo"] == 0
    assert vec_data["vectorElementType"] == VectorElementType.UINT8.value
    assert vec_data["data"] == inp


@given(arrays(dtype=np.uint8, shape=(1, 2)))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_uint8(inp):
    value = value_from_python_types(inp, path="")
    vec_data = value["vectorData"]
    assert vec_data["valueType"] == VectorValueType.VECTOR_DATA.value
    assert vec_data["extraHeaderInfo"] == 0
    assert vec_data["vectorElementType"] == VectorElementType.UINT8.value
    assert vec_data["data"] == inp.tobytes()


@given(arrays(dtype=np.uint16, shape=(1, 2)))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_uint16(inp):
    value = value_from_python_types(inp, path="")
    vec_data = value["vectorData"]
    assert vec_data["valueType"] == VectorValueType.VECTOR_DATA.value
    assert vec_data["extraHeaderInfo"] == 0
    assert vec_data["vectorElementType"] == VectorElementType.UINT16.value
    assert vec_data["data"] == inp.tobytes()


@given(arrays(dtype=np.uint32, shape=(1, 2)))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_uint32(inp):
    value = value_from_python_types(inp, path="")
    vec_data = value["vectorData"]
    assert vec_data["valueType"] == VectorValueType.VECTOR_DATA.value
    assert vec_data["extraHeaderInfo"] == 0
    assert vec_data["vectorElementType"] == VectorElementType.UINT32.value
    assert vec_data["data"] == inp.tobytes()


@given(arrays(dtype=(np.uint64, int), shape=(1, 2)))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_uint64(inp):
    value = value_from_python_types(inp, path="")
    vec_data = value["vectorData"]
    assert vec_data["valueType"] == VectorValueType.VECTOR_DATA.value
    assert vec_data["extraHeaderInfo"] == 0
    assert vec_data["vectorElementType"] == VectorElementType.UINT64.value
    assert vec_data["data"] == inp.tobytes()


@given(arrays(dtype=(float, np.double), shape=(1, 2)))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_double(inp):
    value = value_from_python_types(inp, path="")
    vec_data = value["vectorData"]
    assert vec_data["valueType"] == VectorValueType.VECTOR_DATA.value
    assert vec_data["extraHeaderInfo"] == 0
    assert vec_data["vectorElementType"] == VectorElementType.DOUBLE.value
    assert vec_data["data"] == inp.tobytes()


@given(arrays(dtype=(np.single), shape=(1, 2)))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_float(inp):
    value = value_from_python_types(inp, path="")
    vec_data = value["vectorData"]
    assert vec_data["valueType"] == VectorValueType.VECTOR_DATA.value
    assert vec_data["extraHeaderInfo"] == 0
    assert vec_data["vectorElementType"] == VectorElementType.FLOAT.value
    assert vec_data["data"] == inp.tobytes()


@given(arrays(dtype=(np.csingle), shape=(1, 2)))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_complex_float(inp):
    value = value_from_python_types(inp, path="")
    vec_data = value["vectorData"]
    assert vec_data["valueType"] == VectorValueType.VECTOR_DATA.value
    assert vec_data["extraHeaderInfo"] == 0
    assert vec_data["vectorElementType"] == VectorElementType.COMPLEX_FLOAT.value
    assert vec_data["data"] == inp.tobytes()


def test_value_from_python_types_vector_data_complex_waveform():
    inp = np.array([1 + 2j, 3 + 4j], dtype=np.complex128)
    value = value_from_python_types(inp, path="test/waveforms/0/wave")
    vec_data = value["vectorData"]
    assert vec_data["valueType"] == VectorValueType.VECTOR_DATA.value
    assert vec_data["extraHeaderInfo"] == 0
    assert vec_data["vectorElementType"] == VectorElementType.UINT32.value
    assert vec_data["data"] == preprocess_complex_shf_waveform_vector(inp)[0].tobytes()


@given(arrays(dtype=(np.cdouble), shape=(1, 2)))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_complex_double(inp):
    value = value_from_python_types(inp, path="")
    vec_data = value["vectorData"]
    assert vec_data["valueType"] == VectorValueType.VECTOR_DATA.value
    assert vec_data["extraHeaderInfo"] == 0
    assert vec_data["vectorElementType"] == VectorElementType.COMPLEX_DOUBLE.value
    assert vec_data["data"] == inp.tobytes()


@given(arrays(dtype=(np.bytes_), shape=(1, 2)))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_invalid(inp):
    with pytest.raises(ValueError):
        value_from_python_types(inp, path="")


def test_value_from_python_types_invalid():
    class FakeObject:
        pass

    with pytest.raises(ValueError):
        value_from_python_types(FakeObject, path="")
