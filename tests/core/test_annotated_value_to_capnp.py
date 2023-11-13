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
from labone.core.shf_vector_data import VectorValueType
from labone.core.value import AnnotatedValue


@given(st.integers(min_value=-np.int64(), max_value=np.int64()))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_int64(reflection_server, inp):
    value = AnnotatedValue(value=inp, path="").to_capnp(reflection=reflection_server)
    assert value.value.int64 == inp


@pytest.mark.parametrize(("inp", "out"), [(False, 0), (True, 1)])
def test_value_from_python_types_bool_to_int64(reflection_server, inp, out):
    value = AnnotatedValue(value=inp, path="").to_capnp(reflection=reflection_server)
    assert value.value.int64 is out


@given(st.floats(allow_nan=False))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_double(reflection_server, inp):
    value = AnnotatedValue(value=inp, path="").to_capnp(reflection=reflection_server)
    assert value.value.double == inp


def test_value_from_python_types_np_nan(reflection_server):
    value1 = AnnotatedValue(value=np.nan, path="").to_capnp(
        reflection=reflection_server,
    )
    assert np.isnan(value1.value.double)

    inp = complex(real=0.0, imag=np.nan.imag)
    value2 = AnnotatedValue(value=inp, path="").to_capnp(reflection=reflection_server)
    assert value2.value.complex.real == inp.real
    assert value2.value.complex.imag == inp.imag


@given(st.complex_numbers(allow_nan=False))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_complex(reflection_server, inp):
    value = AnnotatedValue(value=inp, path="").to_capnp(reflection=reflection_server)
    expected = reflection_server.Complex(
        real=inp.real,
        imag=inp.imag,
    )
    assert value.value.complex.real == expected.real
    assert value.value.complex.imag == expected.imag


@given(st.text())
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_string(reflection_server, inp):
    value = AnnotatedValue(value=inp, path="").to_capnp(reflection=reflection_server)
    assert value.value.string == inp


@given(st.binary())
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_bytes(reflection_server, inp):
    value = AnnotatedValue(value=inp, path="").to_capnp(reflection=reflection_server)
    vec_data = value.value.vectorData
    assert vec_data.valueType == VectorValueType.BYTE_ARRAY.value
    assert vec_data.extraHeaderInfo == 0
    assert vec_data.vectorElementType == VectorElementType.UINT8.value
    assert vec_data.data == inp


@given(arrays(dtype=np.uint8, shape=(1, 2)))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_uint8(reflection_server, inp):
    value = AnnotatedValue(value=inp, path="").to_capnp(reflection=reflection_server)
    vec_data = value.value.vectorData
    assert vec_data.valueType == VectorValueType.VECTOR_DATA.value
    assert vec_data.extraHeaderInfo == 0
    assert vec_data.vectorElementType == VectorElementType.UINT8.value
    assert vec_data.data == inp.tobytes()


@given(arrays(dtype=np.uint16, shape=(1, 2)))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_uint16(reflection_server, inp):
    value = AnnotatedValue(value=inp, path="").to_capnp(reflection=reflection_server)
    vec_data = value.value.vectorData
    assert vec_data.valueType == VectorValueType.VECTOR_DATA.value
    assert vec_data.extraHeaderInfo == 0
    assert vec_data.vectorElementType == VectorElementType.UINT16.value
    assert vec_data.data == inp.tobytes()


@given(arrays(dtype=np.uint32, shape=(1, 2)))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_uint32(reflection_server, inp):
    value = AnnotatedValue(value=inp, path="").to_capnp(reflection=reflection_server)
    vec_data = value.value.vectorData
    assert vec_data.valueType == VectorValueType.VECTOR_DATA.value
    assert vec_data.extraHeaderInfo == 0
    assert vec_data.vectorElementType == VectorElementType.UINT32.value
    assert vec_data.data == inp.tobytes()


@given(arrays(dtype=(np.uint64, int), shape=(1, 2)))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_uint64(reflection_server, inp):
    value = AnnotatedValue(value=inp, path="").to_capnp(reflection=reflection_server)
    vec_data = value.value.vectorData
    assert vec_data.valueType == VectorValueType.VECTOR_DATA.value
    assert vec_data.extraHeaderInfo == 0
    assert vec_data.vectorElementType == VectorElementType.UINT64.value
    assert vec_data.data == inp.tobytes()


@given(arrays(dtype=(float, np.double), shape=(1, 2)))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_double(reflection_server, inp):
    value = AnnotatedValue(value=inp, path="").to_capnp(reflection=reflection_server)
    vec_data = value.value.vectorData
    assert vec_data.valueType == VectorValueType.VECTOR_DATA.value
    assert vec_data.extraHeaderInfo == 0
    assert vec_data.vectorElementType == VectorElementType.DOUBLE.value
    assert vec_data.data == inp.tobytes()


@given(arrays(dtype=(np.single), shape=(1, 2)))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_float(reflection_server, inp):
    value = AnnotatedValue(value=inp, path="").to_capnp(reflection=reflection_server)
    vec_data = value.value.vectorData
    assert vec_data.valueType == VectorValueType.VECTOR_DATA.value
    assert vec_data.extraHeaderInfo == 0
    assert vec_data.vectorElementType == VectorElementType.FLOAT.value
    assert vec_data.data == inp.tobytes()


@given(arrays(dtype=(np.csingle), shape=(1, 2)))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_complex_float(reflection_server, inp):
    value = AnnotatedValue(value=inp, path="").to_capnp(reflection=reflection_server)
    vec_data = value.value.vectorData
    assert vec_data.valueType == VectorValueType.VECTOR_DATA.value
    assert vec_data.extraHeaderInfo == 0
    assert vec_data.vectorElementType == VectorElementType.COMPLEX_FLOAT.value
    assert vec_data.data == inp.tobytes()


@given(arrays(dtype=(np.cdouble), shape=(1, 2)))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_complex_double(reflection_server, inp):
    value = AnnotatedValue(value=inp, path="").to_capnp(reflection=reflection_server)
    vec_data = value.value.vectorData
    assert vec_data.valueType == VectorValueType.VECTOR_DATA.value
    assert vec_data.extraHeaderInfo == 0
    assert vec_data.vectorElementType == VectorElementType.COMPLEX_DOUBLE.value
    assert vec_data.data == inp.tobytes()


@given(arrays(dtype=(np.string_), shape=(1, 2)))
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_value_from_python_types_vector_data_invalid(reflection_server, inp):
    with pytest.raises(ValueError):
        AnnotatedValue(value=inp, path="").to_capnp(reflection=reflection_server)


def test_value_from_python_types_invalid(reflection_server):
    class FakeObject:
        pass

    with pytest.raises(ValueError):
        AnnotatedValue(value=FakeObject, path="").to_capnp(reflection=reflection_server)
