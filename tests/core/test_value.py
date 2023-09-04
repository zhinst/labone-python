"""Tests for `AnnotatedValue` and its' parts.

NOTE: `capnp` builder instances cannot be asserter for equality.

TODO: Tests for invalid Python value cases.
"""
import numpy as np
import pytest
from hypothesis import given
from hypothesis import strategies as st
from hypothesis.extra.numpy import arrays
from labone.core.resources import session_protocol_capnp
from labone.core.shf_vector_data import VectorValueType
from labone.core.value import AnnotatedValue, _VectorElementType


class TestAnnotatedValueValue:
    @given(st.integers(min_value=-np.int64(), max_value=np.int64()))
    def test_value_from_python_types_int64(self, inp):
        value = AnnotatedValue(value=inp, path="").to_capnp()
        assert value.value.int64 == inp

    @pytest.mark.parametrize(("inp", "out"), [(False, 0), (True, 1)])
    def test_value_from_python_types_bool_to_int64(self, inp, out):
        value = AnnotatedValue(value=inp, path="").to_capnp()
        assert value.value.int64 is out

    @given(st.floats(allow_nan=False))
    def test_value_from_python_types_double(self, inp):
        value = AnnotatedValue(value=inp, path="").to_capnp()
        assert value.value.double == inp

    def test_value_from_python_types_np_nan(self):
        value1 = AnnotatedValue(value=np.nan, path="").to_capnp()
        assert np.isnan(value1.value.double)

        inp = complex(real=0.0, imag=np.nan.imag)
        value2 = AnnotatedValue(value=inp, path="").to_capnp()
        assert value2.value.complex.real == inp.real
        assert value2.value.complex.imag == inp.imag

    @given(st.complex_numbers(allow_nan=False))
    def test_value_from_python_types_complex(self, inp):
        value = AnnotatedValue(value=inp, path="").to_capnp()
        expected = session_protocol_capnp.Complex(
            real=inp.real,
            imag=inp.imag,
        )
        assert value.value.complex.real == expected.real
        assert value.value.complex.imag == expected.imag

    @given(st.text())
    def test_value_from_python_types_string(self, inp):
        value = AnnotatedValue(value=inp, path="").to_capnp()
        assert value.value.string == inp

    @given(st.binary())
    def test_value_from_python_types_vector_data_bytes(self, inp):
        value = AnnotatedValue(value=inp, path="").to_capnp()
        vec_data = value.value.vectorData
        assert vec_data.valueType == VectorValueType.BYTE_ARRAY.value
        assert vec_data.extraHeaderInfo == 0
        assert vec_data.vectorElementType == _VectorElementType.UINT8.value
        assert vec_data.data == inp

    @given(arrays(dtype=np.uint8, shape=(1, 2)))
    def test_value_from_python_types_vector_data_uint8(self, inp):
        value = AnnotatedValue(value=inp, path="").to_capnp()
        vec_data = value.value.vectorData
        assert vec_data.valueType == VectorValueType.VECTOR_DATA.value
        assert vec_data.extraHeaderInfo == 0
        assert vec_data.vectorElementType == _VectorElementType.UINT8.value
        assert vec_data.data == inp.tobytes()

    @given(arrays(dtype=np.uint16, shape=(1, 2)))
    def test_value_from_python_types_vector_data_uint16(self, inp):
        value = AnnotatedValue(value=inp, path="").to_capnp()
        vec_data = value.value.vectorData
        assert vec_data.valueType == VectorValueType.VECTOR_DATA.value
        assert vec_data.extraHeaderInfo == 0
        assert vec_data.vectorElementType == _VectorElementType.UINT16.value
        assert vec_data.data == inp.tobytes()

    @given(arrays(dtype=np.uint32, shape=(1, 2)))
    def test_value_from_python_types_vector_data_uint32(self, inp):
        value = AnnotatedValue(value=inp, path="").to_capnp()
        vec_data = value.value.vectorData
        assert vec_data.valueType == VectorValueType.VECTOR_DATA.value
        assert vec_data.extraHeaderInfo == 0
        assert vec_data.vectorElementType == _VectorElementType.UINT32.value
        assert vec_data.data == inp.tobytes()

    @given(arrays(dtype=(np.uint64, int), shape=(1, 2)))
    def test_value_from_python_types_vector_data_uint64(self, inp):
        value = AnnotatedValue(value=inp, path="").to_capnp()
        vec_data = value.value.vectorData
        assert vec_data.valueType == VectorValueType.VECTOR_DATA.value
        assert vec_data.extraHeaderInfo == 0
        assert vec_data.vectorElementType == _VectorElementType.UINT64.value
        assert vec_data.data == inp.tobytes()

    @given(arrays(dtype=(float, np.double), shape=(1, 2)))
    def test_value_from_python_types_vector_data_double(self, inp):
        value = AnnotatedValue(value=inp, path="").to_capnp()
        vec_data = value.value.vectorData
        assert vec_data.valueType == VectorValueType.VECTOR_DATA.value
        assert vec_data.extraHeaderInfo == 0
        assert vec_data.vectorElementType == _VectorElementType.DOUBLE.value
        assert vec_data.data == inp.tobytes()

    @given(arrays(dtype=(np.single), shape=(1, 2)))
    def test_value_from_python_types_vector_data_float(self, inp):
        value = AnnotatedValue(value=inp, path="").to_capnp()
        vec_data = value.value.vectorData
        assert vec_data.valueType == VectorValueType.VECTOR_DATA.value
        assert vec_data.extraHeaderInfo == 0
        assert vec_data.vectorElementType == _VectorElementType.FLOAT.value
        assert vec_data.data == inp.tobytes()

    @given(arrays(dtype=(np.csingle), shape=(1, 2)))
    def test_value_from_python_types_vector_data_complex_float(self, inp):
        value = AnnotatedValue(value=inp, path="").to_capnp()
        vec_data = value.value.vectorData
        assert vec_data.valueType == VectorValueType.VECTOR_DATA.value
        assert vec_data.extraHeaderInfo == 0
        assert vec_data.vectorElementType == _VectorElementType.COMPLEX_FLOAT.value
        assert vec_data.data == inp.tobytes()

    @given(arrays(dtype=(np.cdouble), shape=(1, 2)))
    def test_value_from_python_types_vector_data_complex_double(self, inp):
        value = AnnotatedValue(value=inp, path="").to_capnp()
        vec_data = value.value.vectorData
        assert vec_data.valueType == VectorValueType.VECTOR_DATA.value
        assert vec_data.extraHeaderInfo == 0
        assert vec_data.vectorElementType == _VectorElementType.COMPLEX_DOUBLE.value
        assert vec_data.data == inp.tobytes()

    @given(arrays(dtype=(np.string_), shape=(1, 2)))
    def test_value_from_python_types_vector_data_invalid(self, inp):
        with pytest.raises(ValueError):
            AnnotatedValue(value=inp, path="").to_capnp()

    def test_value_from_python_types_invalid(self):
        class FakeObject:
            pass

        with pytest.raises(ValueError):
            AnnotatedValue(value=FakeObject, path="").to_capnp()
