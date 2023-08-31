import struct

import numpy as np
import pytest
from labone.core.resources import session_protocol_capnp
from labone.core.shf_vector_data import (
    VectorValueType,
    get_header_length,
    parse_shf_vector_data_struct,
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


@pytest.mark.parametrize(
    "value_type",
    [
        VectorValueType.SHF_SCOPE_VECTOR_DATA,
        VectorValueType.SHF_DEMODULATOR_VECTOR_DATA,
        VectorValueType.SHF_RESULT_LOGGER_VECTOR_DATA,
    ],
)
def test_missing_extra_header(value_type):
    input_vector = session_protocol_capnp.VectorData.new_message()
    input_vector.valueType = value_type.value
    input_vector.extraHeaderInfo = 0
    with pytest.raises(ValueError):
        parse_shf_vector_data_struct(input_vector)


def _construct_extra_header_value(header_length, major_version, minor_version):
    return int(header_length / 4) | major_version << 21 | minor_version << 16


def test_unsupported_vector_type():
    input_vector = session_protocol_capnp.VectorData.new_message()
    input_vector.valueType = 80
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
def test_invalid_header_version(value_type):
    input_vector = session_protocol_capnp.VectorData.new_message()
    input_vector.valueType = value_type.value
    input_vector.extraHeaderInfo = _construct_extra_header_value(
        header_length=8,
        major_version=0,
        minor_version=0,
    )
    input_vector.data = b"\x00" * 16
    with pytest.raises(NotImplementedError):
        parse_shf_vector_data_struct(input_vector)


@pytest.mark.parametrize("vector_length", range(0, 200, 32))
@pytest.mark.parametrize("scaling", [x * 0.25 for x in range(0, 5)])
@pytest.mark.parametrize("header_version", [1, 2])
@pytest.mark.parametrize(("x", "y"), [(0, 0), (1, 1), (32, 743)])
def test_shf_scope_vector(vector_length, scaling, header_version, x, y):
    input_vector = session_protocol_capnp.VectorData.new_message()
    input_vector.valueType = VectorValueType.SHF_SCOPE_VECTOR_DATA.value
    header_length = 64
    input_vector.extraHeaderInfo = _construct_extra_header_value(
        header_length=header_length,
        major_version=0,
        minor_version=header_version,
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
        assert extra_header.num_missed_triggers == -1
        assert extra_header.num_segments == -1
        assert extra_header.num_total_segments == -1
        assert extra_header.first_segment_index == -1
    else:
        assert extra_header.num_missed_triggers == 0
        assert extra_header.num_segments == 0
        assert extra_header.num_total_segments == 0
        assert extra_header.first_segment_index == 0


@pytest.mark.parametrize("vector_length", range(0, 200, 32))
@pytest.mark.parametrize("scaling", [x * 0.25 for x in range(0, 5)])
@pytest.mark.parametrize("timestamp_diff", range(0, 100, 25))
@pytest.mark.parametrize("header_version", [1, 2])
@pytest.mark.parametrize(("x", "y"), [(0, 0), (1, 1), (32, 743)])
def test_shf_demodulator_vector(  # noqa: PLR0913
    vector_length,
    scaling,
    timestamp_diff,
    header_version,
    x,
    y,
):
    input_vector = session_protocol_capnp.VectorData.new_message()
    input_vector.valueType = VectorValueType.SHF_DEMODULATOR_VECTOR_DATA.value
    header_length = 64
    input_vector.extraHeaderInfo = _construct_extra_header_value(
        header_length=header_length,
        major_version=0,
        minor_version=header_version,
    )
    # Manually set the scaling Factor
    header = (
        b"\x00" * 8
        + struct.pack("I", timestamp_diff)
        + b"\x00" * 20
        + struct.pack("d", scaling)
        + b"\x00"
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
    assert extra_header.timestamp_diff == timestamp_diff * 80
    assert extra_header.abort_config is False
    assert extra_header.trigger_source == 0
    assert extra_header.trigger_length == 0
    assert extra_header.trigger_index == 0
    assert extra_header.trigger_tag == 0
    assert extra_header.awg_tag == 0
    assert extra_header.scaling == scaling
    assert extra_header.center_freq == 0.0
    # Unknown values are marked with -1
    if header_version == 1:
        assert extra_header.oscillator_source == -1
        assert extra_header.signal_source == -1
    else:
        assert extra_header.oscillator_source == 0
        assert extra_header.signal_source == 0


@pytest.mark.parametrize("vector_length", range(0, 200, 32))
@pytest.mark.parametrize("header_version", [1])
@pytest.mark.parametrize("x", range(0, 30, 7))
def test_shf_result_logger_vector(vector_length, header_version, x):
    input_vector = session_protocol_capnp.VectorData.new_message()
    input_vector.valueType = VectorValueType.SHF_RESULT_LOGGER_VECTOR_DATA.value
    input_vector.vectorElementType = 2  # uint32
    header_length = 32
    input_vector.extraHeaderInfo = _construct_extra_header_value(
        header_length=header_length,
        major_version=0,
        minor_version=header_version,
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
    assert extra_header.timestamp_diff == 0
    assert extra_header.scaling == 0.0
    assert extra_header.center_freq == 0.0


@pytest.mark.parametrize("vector_length", range(0, 200, 32))
@pytest.mark.parametrize(("x", "y"), [(0, 0), (1, 1), (32, 743), (3785687, 1285732)])
def test_shf_waveform_logger_vector(vector_length, x, y):
    input_vector = session_protocol_capnp.VectorData.new_message()
    input_vector.valueType = VectorValueType.SHF_GENERATOR_WAVEFORM_VECTOR_DATA.value
    input_vector.vectorElementType = 2  # uint32
    input_vector.extraHeaderInfo = 0
    input_vector.data = (struct.pack("I", x) + struct.pack("I", y)) * vector_length
    output_vector, extra_header = parse_shf_vector_data_struct(input_vector)
    const_scaling = 1 / 131071.0  # constant scaling factor based on the encoding bits
    assert np.array_equal(
        output_vector,
        x * const_scaling
        + 1j * const_scaling * y * np.ones(vector_length, dtype=np.complex128),
    )
    assert extra_header is None
