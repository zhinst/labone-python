import struct

import numpy as np
import pytest

from labone.core import hpk_schema
from labone.core.helper import VectorElementType
from labone.core.shf_vector_data import (
    #     SHFDemodSample,
    #     ShfDemodulatorVectorExtraHeader,
    #     ShfResultLoggerVectorExtraHeader,
    #     ShfScopeVectorExtraHeader,
    ShfGeneratorWaveformVectorData,
    ShfResultLoggerVectorData,
    ShfScopeVectorData,
    VectorValueType,
    #     encode_shf_vector_data_struct,
    get_header_length,
    parse_shf_vector_from_vector_data,
    #     preprocess_complex_shf_waveform_vector,
)


class FakeShfVectorDataStruct:
    def __init__(self, extra_header_info=None):
        self.extraHeaderInfo = extra_header_info


@pytest.mark.parametrize(
    ("header_bytes", "expected_length"),
    [
        (b"\x00\x00\x00\x00", 0),
        (b"\x01\x00\x00\x00", 0),
        (b"\x02\x02\x00\x00", 0),
        (b"\x02\x02\x02\x00", 2048),
        (b"\x00\x00\x00\x01", 4),
        (b"\x02\x02\x02\x00\x02\x02\x02\x00", 2048),
        (b"\x02\x02\x02\x02\x02\x02\x02\x02", 2056),
        (b"\x00\x00\x00\x00\x00\x00\x00\x01", 4),
        (b"\x00\x00\x00\x00\x00\x00\x00\x05", 20),
    ],
)
def test_get_header_length(header_bytes, expected_length):
    assert (
        get_header_length(
            FakeShfVectorDataStruct(
                extra_header_info=int.from_bytes(bytes=header_bytes, byteorder="big"),
            ),
        )
        == expected_length
    )


def _to_vector_data(
    value_type: int,
    data: bytes = b"",
    extra_header_info: int = 0,
    vector_element_type: VectorElementType = VectorElementType.UINT8,
):
    msg = hpk_schema.VectorData()
    msg.valueType = value_type
    msg.vectorElementType = vector_element_type
    msg.extraHeaderInfo = extra_header_info
    msg.data = data
    return msg


@pytest.mark.parametrize(
    "value_type",
    [
        VectorValueType.SHF_SCOPE_VECTOR_DATA,
        VectorValueType.SHF_RESULT_LOGGER_VECTOR_DATA,
        VectorValueType.SHF_GENERATOR_WAVEFORM_VECTOR_DATA,
    ],
)
def test_missing_extra_header(value_type):
    input_vector = _to_vector_data(value_type=value_type.value)
    parse_shf_vector_from_vector_data(input_vector)


def test_unsupported_vector_type():
    input_vector = _to_vector_data(value_type=80)
    with pytest.raises(ValueError):
        parse_shf_vector_from_vector_data(input_vector)


def _construct_extra_header_value(header_length, major_version, minor_version):
    return int(header_length / 4) | major_version << 21 | minor_version << 16


@pytest.mark.parametrize(
    "value_type",
    [
        VectorValueType.SHF_SCOPE_VECTOR_DATA,
        VectorValueType.SHF_DEMODULATOR_VECTOR_DATA,
        VectorValueType.SHF_RESULT_LOGGER_VECTOR_DATA,
    ],
)
def test_invalid_header_version(
    value_type,
):
    input_vector = _to_vector_data(
        value_type=value_type.value,
        extra_header_info=_construct_extra_header_value(
            header_length=8,
            major_version=0,
            minor_version=0,
        ),
        data=b"\x00" * 16,
    )
    with pytest.raises(Exception):  # noqa: B017
        parse_shf_vector_from_vector_data(input_vector)


@pytest.mark.parametrize("vector_length", range(0, 200, 32))
@pytest.mark.parametrize("scaling", [x * 0.25 for x in range(5)])
@pytest.mark.parametrize("header_version", [1, 2])
@pytest.mark.parametrize(("x", "y"), [(0, 0), (1, 1), (32, 743)])
def test_shf_scope_vector(
    vector_length,
    scaling,
    header_version,
    x,
    y,
):
    header_length = 64
    # Manually set the scaling Factor
    header = b"\x00" * 16 + struct.pack("d", scaling) + b"\x00"
    header = header + b"\x00" * (header_length - len(header))
    # The data are interleave complex integers
    data = (struct.pack("I", x) + struct.pack("I", y)) * vector_length

    input_vector = _to_vector_data(
        value_type=VectorValueType.SHF_SCOPE_VECTOR_DATA.value,
        extra_header_info=_construct_extra_header_value(
            header_length=header_length,
            major_version=0,
            minor_version=header_version,
        ),
        data=header + data,
    )
    output_vector = parse_shf_vector_from_vector_data(input_vector)
    assert isinstance(output_vector, ShfScopeVectorData)
    assert np.array_equal(
        output_vector.vector,
        x * scaling + 1j * y * scaling * np.ones(vector_length, dtype=np.complex128),
    )

    assert output_vector.properties.averageCount == 0
    assert output_vector.properties.centerFrequency == 0.0
    assert output_vector.properties.inputSelect == 0
    assert output_vector.properties.flags == 0
    assert output_vector.properties.scaling == scaling
    assert output_vector.properties.timestamp == 0
    assert output_vector.properties.timestampDiff == 0
    assert output_vector.properties.triggerTimestamp == 0
    assert output_vector.properties.numMissedTriggers == 0
    assert output_vector.properties.numSegments == 0
    assert output_vector.properties.numTotalSegments == 0
    assert output_vector.properties.firstSegmentIndex == 0


@pytest.mark.parametrize("vector_length", range(0, 200, 32))
@pytest.mark.parametrize("header_version", [1])
@pytest.mark.parametrize("x", range(0, 30, 7))
def test_shf_result_logger_vector(vector_length, header_version, x):
    header_length = 72
    header = b"\x00" * header_length
    data = struct.pack("I", x) * vector_length
    input_vector = _to_vector_data(
        value_type=VectorValueType.SHF_RESULT_LOGGER_VECTOR_DATA.value,
        vector_element_type=2,
        extra_header_info=_construct_extra_header_value(
            header_length=header_length,
            major_version=0,
            minor_version=header_version,
        ),
        data=header + data,
    )

    output_vector = parse_shf_vector_from_vector_data(input_vector)

    assert isinstance(output_vector, ShfResultLoggerVectorData)
    assert np.array_equal(
        output_vector.vector,
        x * np.ones(vector_length, dtype=np.uint32),
    )

    assert output_vector.properties.timestamp == 0
    assert output_vector.properties.timestamp == 0
    assert output_vector.properties.jobId == 0
    assert output_vector.properties.repetitionId == 0
    assert output_vector.properties.scaling == 0.0
    assert output_vector.properties.centerFrequency == 0.0
    assert output_vector.properties.dataSource == 0
    assert output_vector.properties.numSamples == 0
    assert output_vector.properties.numSpectrSamples == 0
    assert output_vector.properties.numAverages == 0
    assert output_vector.properties.numAcquired == 0
    assert output_vector.properties.holdoffErrorsReslog == 0
    assert output_vector.properties.holdoffErrorsReadout == 0
    assert output_vector.properties.holdoffErrorsSpectr == 0


@pytest.mark.parametrize("vector_length", range(0, 200, 32))
@pytest.mark.parametrize(("x", "y"), [(0, 0), (1, 1), (32, 743), (3785687, 1285732)])
def test_shf_waveform_logger_vector(vector_length, x, y):
    input_vector = _to_vector_data(
        value_type=VectorValueType.SHF_GENERATOR_WAVEFORM_VECTOR_DATA.value,
        vector_element_type=2,  # uint32
        extra_header_info=0,
        data=(struct.pack("I", x) + struct.pack("I", y)) * vector_length,
    )
    output_vector = parse_shf_vector_from_vector_data(input_vector)
    assert isinstance(output_vector, ShfGeneratorWaveformVectorData)
    const_scaling = 1 / 131071.0  # constant scaling factor based on the encoding bits
    assert np.array_equal(
        output_vector.complex,
        x * const_scaling
        + 1j * const_scaling * y * np.ones(vector_length, dtype=np.complex128),
    )
