import struct

import numpy as np
import pytest

from labone.core.helper import VectorElementType
from labone.core.shf_vector_data import (
    SHFDemodSample,
    ShfDemodulatorVectorExtraHeader,
    ShfResultLoggerVectorExtraHeader,
    ShfScopeVectorExtraHeader,
    VectorValueType,
    encode_shf_vector_data_struct,
    get_header_length,
    parse_shf_vector_data_struct,
    preprocess_complex_shf_waveform_vector,
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


class VectorData:
    def __init__(
        self,
        valueType,  # noqa: N803
        extraHeaderInfo=0,  # noqa: N803
        data=None,
        vectorElementType=VectorElementType.UINT8,  # noqa: N803
    ):
        self.valueType = valueType
        self.extraHeaderInfo = extraHeaderInfo
        self.data = [] if data is None else data
        self.vectorElementType = vectorElementType


@pytest.mark.parametrize(
    "value_type",
    [
        VectorValueType.SHF_SCOPE_VECTOR_DATA,
        VectorValueType.SHF_DEMODULATOR_VECTOR_DATA,
        VectorValueType.SHF_RESULT_LOGGER_VECTOR_DATA,
    ],
)
def test_missing_extra_header(value_type):
    input_vector = VectorData(valueType=value_type.value, extraHeaderInfo=0)
    result = parse_shf_vector_data_struct(input_vector)
    assert result[1] is None


def _construct_extra_header_value(header_length, major_version, minor_version):
    return int(header_length / 4) | major_version << 21 | minor_version << 16


def test_unsupported_vector_type():
    input_vector = VectorData(valueType=80)
    with pytest.raises(ValueError):
        parse_shf_vector_data_struct(input_vector)


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
    input_vector = VectorData(
        valueType=value_type.value,
        extraHeaderInfo=_construct_extra_header_value(
            header_length=8,
            major_version=0,
            minor_version=0,
        ),
        data=b"\x00" * 16,
    )
    with pytest.raises(Exception):  # noqa: B017
        parse_shf_vector_data_struct(input_vector)


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
    input_vector = VectorData(
        valueType=VectorValueType.SHF_SCOPE_VECTOR_DATA.value,
        extraHeaderInfo=_construct_extra_header_value(
            header_length=header_length,
            major_version=0,
            minor_version=header_version,
        ),
    )
    # Manually set the scaling Factor
    header = b"\x00" * 16 + struct.pack("d", scaling) + b"\x00"
    header = header + b"\x00" * (header_length - len(header))
    # The data are interleave complex integers
    data = (struct.pack("I", x) + struct.pack("I", y)) * vector_length
    input_vector.data = header + data
    output_vector, extra_header = parse_shf_vector_data_struct(input_vector)
    assert np.array_equal(
        output_vector,
        x * scaling + 1j * y * scaling * np.ones(vector_length, dtype=np.complex128),
    )

    assert extra_header.average_count == 0
    assert extra_header.center_freq == 0.0
    assert extra_header.input_select == 0
    assert extra_header.interleaved is False
    assert extra_header.scaling == scaling
    assert extra_header.timestamp == 0
    assert extra_header.timestamp_diff == 0
    assert extra_header.trigger_timestamp == 0
    # Unknown values are marked with -1
    if header_version == 1:
        assert extra_header.missed_triggers == -1
        assert extra_header.segments == -1
        assert extra_header.total_segments == -1
        assert extra_header.first_segment_index == -1
    else:
        assert extra_header.missed_triggers == 0
        assert extra_header.segments == 0
        assert extra_header.total_segments == 0
        assert extra_header.first_segment_index == 0


@pytest.mark.parametrize("vector_length", range(0, 200, 32))
@pytest.mark.parametrize("scaling", [x * 0.25 for x in range(5)])
@pytest.mark.parametrize("timestamp_delta", range(0, 100, 25))
@pytest.mark.parametrize(
    "header_version",
    [
        0,
    ],
)
@pytest.mark.parametrize(("x", "y"), [(0, 0), (1, 1), (32, 743)])
def test_shf_demodulator_vector(
    vector_length,
    scaling,
    timestamp_delta,
    header_version,
    x,
    y,
):
    header_length = 64
    input_vector = VectorData(
        valueType=VectorValueType.SHF_DEMODULATOR_VECTOR_DATA.value,
        extraHeaderInfo=_construct_extra_header_value(
            header_length=header_length,
            major_version=1,
            minor_version=header_version,
        ),
    )
    # Manually set the scaling Factor
    header = (
        b"\x00" * 8
        + struct.pack("I", timestamp_delta)
        + b"\x00" * 36
        + struct.pack("f", scaling)
    )

    header = header + b"\x00" * (header_length - len(header))
    # The data are interleave complex integers
    data = (struct.pack("q", x) + struct.pack("q", y)) * vector_length
    input_vector.data = header + data
    output_vector, extra_header = parse_shf_vector_data_struct(input_vector)

    assert np.array_equal(
        output_vector.x,
        x * scaling * np.ones(vector_length, dtype=np.int64),
    )
    assert np.array_equal(
        output_vector.y,
        y * scaling * np.ones(vector_length, dtype=np.int64),
    )

    assert extra_header.timestamp == 0
    assert extra_header.timestamp_delta == timestamp_delta
    assert extra_header.burst_length == 0
    assert extra_header.burst_offset == 0
    assert extra_header.trigger_index == 0
    assert extra_header.trigger_timestamp == 0
    assert extra_header.center_freq == 0
    assert extra_header.rf_path is False
    assert extra_header.oscillator_source == 0
    assert extra_header.harmonic == 0
    assert extra_header.trigger_source == 0
    assert extra_header.signal_source == 0
    assert extra_header.oscillator_freq == 0
    assert extra_header.scaling == scaling


@pytest.mark.parametrize("vector_length", range(0, 200, 32))
@pytest.mark.parametrize("header_version", [1])
@pytest.mark.parametrize("x", range(0, 30, 7))
def test_shf_result_logger_vector(vector_length, header_version, x):
    header_length = 72
    input_vector = VectorData(
        valueType=VectorValueType.SHF_RESULT_LOGGER_VECTOR_DATA.value,
        vectorElementType=2,
        extraHeaderInfo=_construct_extra_header_value(
            header_length=header_length,
            major_version=0,
            minor_version=header_version,
        ),
    )
    header = b"\x00" * header_length
    data = struct.pack("I", x) * vector_length
    input_vector.data = header + data
    output_vector, extra_header = parse_shf_vector_data_struct(input_vector)
    assert np.array_equal(
        output_vector,
        x * np.ones(vector_length, dtype=np.uint32),
    )

    assert extra_header.timestamp == 0
    assert extra_header.timestamp == 0
    assert extra_header.job_id == 0
    assert extra_header.repetition_id == 0
    assert extra_header.scaling == 0.0
    assert extra_header.center_freq == 0.0
    assert extra_header.data_source == 0
    assert extra_header.samples == 0
    assert extra_header.spectr_samples == 0
    assert extra_header.averages == 0
    assert extra_header.acquired == 0
    assert extra_header.holdoff_errors_reslog == 0
    assert extra_header.holdoff_errors_readout == 0
    assert extra_header.holdoff_errors_spectr == 0


@pytest.mark.parametrize("vector_length", range(0, 200, 32))
@pytest.mark.parametrize(("x", "y"), [(0, 0), (1, 1), (32, 743), (3785687, 1285732)])
def test_shf_waveform_logger_vector(vector_length, x, y):
    input_vector = VectorData(
        valueType=VectorValueType.SHF_GENERATOR_WAVEFORM_VECTOR_DATA.value,
        vectorElementType=2,  # uint32
        extraHeaderInfo=0,
        data=(struct.pack("I", x) + struct.pack("I", y)) * vector_length,
    )
    output_vector, extra_header = parse_shf_vector_data_struct(input_vector)
    const_scaling = 1 / 131071.0  # constant scaling factor based on the encoding bits
    assert np.array_equal(
        output_vector,
        x * const_scaling
        + 1j * const_scaling * y * np.ones(vector_length, dtype=np.complex128),
    )
    assert extra_header is None


@pytest.mark.parametrize(
    "header",
    [
        ShfScopeVectorExtraHeader(
            timestamp=21,
            timestamp_diff=22,
            interleaved=False,
            scaling=3.0,
            average_count=7,
            center_freq=23,
            input_select=24,
            missed_triggers=25,
            segments=26,
            total_segments=27,
            first_segment_index=28,
            trigger_timestamp=29,
        ),
        ShfResultLoggerVectorExtraHeader(
            timestamp=1,
            job_id=2,
            repetition_id=3,
            scaling=50,
            center_freq=4,
            data_source=5,
            samples=6,
            spectr_samples=7,
            averages=8,
            acquired=9,
            holdoff_errors_reslog=10,
            holdoff_errors_readout=11,
            holdoff_errors_spectr=12,
            first_sample_timestamp=13,
        ),
        ShfDemodulatorVectorExtraHeader(
            timestamp=0,
            timestamp_delta=0,
            burst_length=4,
            burst_offset=5,
            trigger_index=6,
            trigger_timestamp=7,
            center_freq=8,
            rf_path=True,
            oscillator_source=3,
            harmonic=10,
            trigger_source=2,
            signal_source=4,
            oscillator_freq=13,
            scaling=4.0000000467443897e-07,
        ),
    ],
)
@pytest.mark.asyncio()
async def test_header_to_binary_from_binary_invers(header):
    binary, encoding_version = header.to_binary()
    assert header == header.__class__.from_binary(
        binary=binary,
        version=encoding_version,
    )


class GetAttrAbleDict(dict):
    def __getattr__(self, item):
        return self[item]


@pytest.mark.parametrize(
    ("header", "data"),
    [
        (
            ShfScopeVectorExtraHeader(
                timestamp=0,
                timestamp_diff=0,
                interleaved=False,
                scaling=3.0,
                average_count=7,
                center_freq=23,
                input_select=24,
                missed_triggers=25,
                segments=26,
                total_segments=27,
                first_segment_index=28,
                trigger_timestamp=29,
            ),
            np.array([6 + 6j, 3 + 3j], dtype=np.complex64),
        ),
        (
            ShfResultLoggerVectorExtraHeader(
                timestamp=1,
                job_id=2,
                repetition_id=3,
                scaling=50,
                center_freq=4,
                data_source=5,
                samples=6,
                spectr_samples=7,
                averages=8,
                acquired=9,
                holdoff_errors_reslog=10,
                holdoff_errors_readout=11,
                holdoff_errors_spectr=12,
                first_sample_timestamp=13,
            ),
            np.array([50 + 100j, 100 + 150j], dtype=np.complex64),
        ),
    ],
)
def test_encoding_decoding_are_invers(header, data):
    data_copy = data.copy()
    capnp = encode_shf_vector_data_struct(
        data=data,
        extra_header=header,
    )
    inp = GetAttrAbleDict()
    inp.update(capnp)
    extracted_data, extracted_header = parse_shf_vector_data_struct(inp)

    assert extracted_header == header
    assert np.array_equal(extracted_data, data_copy)


@pytest.mark.parametrize(
    ("header", "data"),
    [
        (
            ShfDemodulatorVectorExtraHeader(
                timestamp=0,
                timestamp_delta=0,
                burst_length=4,
                burst_offset=5,
                trigger_index=6,
                trigger_timestamp=7,
                center_freq=8,
                rf_path=True,
                oscillator_source=3,
                harmonic=10,
                trigger_source=2,
                signal_source=4,
                oscillator_freq=13,
                scaling=4.0000000467443897e-07,
            ),
            SHFDemodSample(
                np.array([6, 4], dtype=np.int64),
                np.array([8, 2], dtype=np.int64),
            ),
        ),
    ],
)
def test_encoding_decoding_are_invers_shf_demod_sample(header, data):
    data_copy = SHFDemodSample(data.x.copy(), data.y.copy())

    capnp = encode_shf_vector_data_struct(
        data=data,
        extra_header=header,
    )
    inp = GetAttrAbleDict()
    inp.update(capnp)
    extracted_data, extracted_header = parse_shf_vector_data_struct(inp)

    assert extracted_header == header
    assert np.allclose(extracted_data.x, data_copy.x, atol=1e-7)
    assert np.allclose(extracted_data.y, data_copy.y, atol=1e-7)


@pytest.mark.parametrize(
    ("header", "data"),
    [
        (
            ShfDemodulatorVectorExtraHeader(
                timestamp=0,
                timestamp_delta=0,
                burst_length=4,
                burst_offset=5,
                trigger_index=6,
                trigger_timestamp=7,
                center_freq=8,
                rf_path=True,
                oscillator_source=3,
                harmonic=10,
                trigger_source=2,
                signal_source=4,
                oscillator_freq=13,
                scaling=4e-07,
            ),
            np.array([50 + 100j, 100 + 150j], dtype=np.complex64),
        ),
        (
            ShfScopeVectorExtraHeader(
                timestamp=0,
                timestamp_diff=0,
                interleaved=False,
                scaling=3.0,
                average_count=7,
                center_freq=23,
                input_select=24,
                missed_triggers=25,
                segments=26,
                total_segments=27,
                first_segment_index=28,
                trigger_timestamp=29,
            ),
            SHFDemodSample(
                np.array([6, 4], dtype=np.int64),
                np.array([8, 2], dtype=np.int64),
            ),
        ),
        (
            ShfResultLoggerVectorExtraHeader(0, 0, 0, 50, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            SHFDemodSample(
                np.array([6, 3], dtype=np.int64),
                np.array([7, 2], dtype=np.int64),
            ),
        ),
    ],
)
@pytest.mark.asyncio()
async def test_encode_shf_vector_wrong_data_header_combination_raises(header, data):
    with pytest.raises(Exception):  # noqa: B017
        encode_shf_vector_data_struct(data=data, extra_header=header)


@pytest.mark.parametrize(
    ("source", "target"),
    [
        (
            np.array([1 + 1j, 2 + 2j, 3 + 3j, 4 + 4j], dtype=np.complex64),
            np.array(
                [131071, 131071, 262142, 262142, 393213, 393213, 524284, 524284],
                dtype=np.complex64,
            ),
        ),
        (
            np.array([1 + 2j, 3 + 4j, 5 + 6j, 7 + 8j], dtype=np.complex64),
            np.array(
                [131071, 262142, 393213, 524284, 655355, 786426, 917497, 1048568],
                dtype=np.complex64,
            ),
        ),
    ],
)
def test_preprocess_complex_shf_waveform_vector(source, target):
    result, result_type = preprocess_complex_shf_waveform_vector(data=source)
    assert result_type == np.uint32
    assert np.array_equal(result, target)
