"""Tests for `AnnotatedValue` and its' parts.

NOTE: `capnp` builder instances cannot be asserter for equality.

TODO: Tests for invalid Python value cases.
"""
import numpy as np
import pytest
from hypothesis import given
from hypothesis import strategies as st
from hypothesis.extra.numpy import arrays
from labone.core import value
from labone.core.resources import session_protocol_capnp


class TestAnnotatedValueFromPyTypes:
    @given(st.integers(min_value=-np.int64(), max_value=np.int64()))
    def test_value_from_python_types_int64(self, inp):
        assert value._value_from_python_types(inp).int64 == inp

    @pytest.mark.parametrize(("inp", "out"), [(False, 0), (True, 1)])
    def test_value_from_python_types_bool_to_int64(self, inp, out):
        assert value._value_from_python_types(inp).int64 == out

    @given(st.floats(allow_nan=False))
    def test_value_from_python_types_double(self, inp):
        assert value._value_from_python_types(inp).double == inp

    def test_value_from_python_types_np_nan(self):
        rval = value._value_from_python_types(np.nan).double
        assert np.nan_to_num(rval) == 0.0

        inp = complex(real=0.0, imag=np.nan.imag)
        out = value._value_from_python_types(inp).complex
        assert inp.real == np.nan_to_num(out.real)
        assert inp.imag == np.nan.imag

    @given(st.complex_numbers(allow_nan=False))
    def test_value_from_python_types_complex(self, inp):
        obj = value._value_from_python_types(inp).complex
        expected = session_protocol_capnp.Complex(
            real=inp.real,
            imag=inp.imag,
        )
        assert obj.real == expected.real
        assert obj.imag == expected.imag

    @given(st.text())
    def test_value_from_python_types_string(self, inp):
        rval = value._value_from_python_types(inp)
        assert rval.string == inp

    @given(st.binary())
    def test_value_from_python_types_vector_data_bytes(self, vec):
        rval = value._value_from_python_types(vec).vectorData
        assert rval.valueType == value.VectorValueType.BYTE_ARRAY.value
        assert rval.extraHeaderInfo == 0
        assert rval.vectorElementType == value.VectorElementType.UINT8.value
        assert rval.data == vec

    @given(arrays(dtype=np.uint8, shape=(1, 2)))
    def test_value_from_python_types_vector_data_uint8(self, vec):
        rval = value._value_from_python_types(vec).vectorData
        assert rval.valueType == value.VectorValueType.VECTOR_DATA.value
        assert rval.extraHeaderInfo == 0
        assert rval.vectorElementType == value.VectorElementType.UINT8.value
        assert rval.data == vec.tobytes()

    @given(arrays(dtype=np.uint16, shape=(1, 2)))
    def test_value_from_python_types_vector_data_uint16(self, vec):
        rval = value._value_from_python_types(vec).vectorData
        assert rval.valueType == value.VectorValueType.VECTOR_DATA.value
        assert rval.extraHeaderInfo == 0
        assert rval.vectorElementType == value.VectorElementType.UINT16.value
        assert rval.data == vec.tobytes()

    @given(arrays(dtype=np.uint32, shape=(1, 2)))
    def test_value_from_python_types_vector_data_uint32(self, vec):
        rval = value._value_from_python_types(vec).vectorData
        assert rval.valueType == value.VectorValueType.VECTOR_DATA.value
        assert rval.extraHeaderInfo == 0
        assert rval.vectorElementType == value.VectorElementType.UINT32.value
        assert rval.data == vec.tobytes()

    @given(arrays(dtype=(np.uint64, int), shape=(1, 2)))
    def test_value_from_python_types_vector_data_uint64(self, vec):
        rval = value._value_from_python_types(vec).vectorData
        assert rval.valueType == value.VectorValueType.VECTOR_DATA.value
        assert rval.extraHeaderInfo == 0
        assert rval.vectorElementType == value.VectorElementType.UINT64.value
        assert rval.data == vec.tobytes()

    @given(arrays(dtype=(float, np.double), shape=(1, 2)))
    def test_value_from_python_types_vector_data_double(self, vec):
        rval = value._value_from_python_types(vec).vectorData
        assert rval.valueType == value.VectorValueType.VECTOR_DATA.value
        assert rval.extraHeaderInfo == 0
        assert rval.vectorElementType == value.VectorElementType.DOUBLE.value
        assert rval.data == vec.tobytes()

    @given(arrays(dtype=(np.single), shape=(1, 2)))
    def test_value_from_python_types_vector_data_float(self, vec):
        rval = value._value_from_python_types(vec).vectorData
        assert rval.valueType == value.VectorValueType.VECTOR_DATA.value
        assert rval.extraHeaderInfo == 0
        assert rval.vectorElementType == value.VectorElementType.FLOAT.value
        assert rval.data == vec.tobytes()

    @given(arrays(dtype=(np.csingle), shape=(1, 2)))
    def test_value_from_python_types_vector_data_complex_float(self, vec):
        rval = value._value_from_python_types(vec).vectorData
        assert rval.valueType == value.VectorValueType.VECTOR_DATA.value
        assert rval.extraHeaderInfo == 0
        assert rval.vectorElementType == value.VectorElementType.COMPLEX_FLOAT.value
        assert rval.data == vec.tobytes()

    @given(arrays(dtype=(np.cdouble), shape=(1, 2)))
    def test_value_from_python_types_vector_data_complex_double(self, vec):
        rval = value._value_from_python_types(vec).vectorData
        assert rval.valueType == value.VectorValueType.VECTOR_DATA.value
        assert rval.extraHeaderInfo == 0
        assert rval.vectorElementType == value.VectorElementType.COMPLEX_DOUBLE.value
        assert rval.data == vec.tobytes()
