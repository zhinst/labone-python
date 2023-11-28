"""Module for parsing shf vector data.

This module duplicates the functionality of the C++ client. It is planed to
move this logic into the kernel/server. This module should be removed once
the kernel/server is updated.

The reason why shf vector types need special handling is because the device
send for simplicity a single byte array for both the data and the extra header.
The extra header is a struct with a variable length. The length of the extra
header is encoded in the 16 least significant bits of the extraHeaderInfo
field of the vector data. The extra header is then followed by the data.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Union

import numpy as np

from labone.core.helper import CapnpCapability, VectorElementType, VectorValueType


@dataclass(frozen=True)
class _HeaderVersion:
    """Class for the version of the extra header."""

    major: int
    minor: int


@dataclass
class SHFDemodSample:
    """SHF demodulator sample data."""

    x: np.ndarray
    y: np.ndarray


@dataclass
class ShfResultLoggerVectorExtraHeader:
    """Class for the extra header of result logger vectors.

    Args:
        timestamp: Timestamp of the first sample of the current block.
            When averaging is turned on, this is the timestamp of the first shot.
        timestamp_diff: Timestamp delta between subsequent samples
        scaling: scaling value, used to scale the vector data to the desired
            range (e.g. from raw ADC values into a +/-1 full-scale range)
        center_freq: RF center frequency. This field corresponds to the
            following node /dev.../qachannels/0/centerfreq
    """

    timestamp: int
    job_id: int
    repetition_id: int
    scaling: float
    center_freq: float
    data_source: int
    num_samples: int
    num_spectr_samples: int
    num_averages: int
    num_acquired: int
    holdoff_errors_reslog: int
    holdoff_errors_readout: int
    holdoff_errors_spectr: int

    @staticmethod
    def from_binary(
        binary: bytes,
        *,
        version: _HeaderVersion,
    ) -> ShfResultLoggerVectorExtraHeader:
        """Parse the extra header of result logger vectors.

        Copy of CoreShfNodeData.cpp

        Args:
            binary: The binary string representing the extra header.
            version: The version of the extra header.

        Returns:
            The parsed extra header.

        Raises:
            NotImplementedError: If the version is not supported.
        """
        if (version.major == 0) and (version.minor >= 1):
            return ShfResultLoggerVectorExtraHeader(
                timestamp=struct.unpack("q", binary[0:8])[0],
                job_id=struct.unpack("I", binary[8:12])[0],
                repetition_id=struct.unpack("I", binary[12:16])[0],
                scaling=struct.unpack("d", binary[16:24])[0],
                center_freq=struct.unpack("d", binary[24:32])[0],
                data_source=struct.unpack("I", binary[32:36])[0],
                num_samples=struct.unpack("I", binary[36:40])[0],
                num_spectr_samples=struct.unpack("I", binary[40:44])[0],
                num_averages=struct.unpack("I", binary[44:48])[0],
                num_acquired=struct.unpack("I", binary[48:52])[0],
                holdoff_errors_reslog=struct.unpack("H", binary[52:54])[0],
                holdoff_errors_readout=struct.unpack("H", binary[54:56])[0],
                holdoff_errors_spectr=struct.unpack("H", binary[56:58])[0],
            )
        msg = str(
            f"Unsupported extra header version: {version} for "
            "ShfResultLoggerVectorExtraHeader",
        )
        raise NotImplementedError(msg)


@dataclass
class ShfScopeVectorExtraHeader:
    """Class for the extra header of scope vectors.

    Args:
        timestamp: Timestamp of the first sample of the current block.
            When averaging is turned on, this is the timestamp of the first shot.
        timestamp_diff: Timestamp delta between subsequent samples
        interleaved: Flag if the vector contains complex numbers (interleaved)
            or real numbers
        scaling: Scaling value to convert the measurement data from 32-bit
            signed integer to double
        center_freq: configured center frequency
        trigger_timestamp: Timestamp of trigger
        input_select: Input Select used for this scope channel
        average_count: Number of recordings taken into account for building
            the average
        num_segments: 	Number of segments contained in the scope vector
        num_total_segments: Total number of segments. This allows sending
            partial results by setting num_segments < num_total_segments
        first_segment_index: Index of the first segment in the scope vector
        num_missed_triggers: Number of missed triggers due to hold-off time
            violations. Note a missed trigger count greater than 0 means
            that the segment data will likely be invalid as we record one
            segment per trigger!
    """

    timestamp: int
    timestamp_diff: int
    interleaved: bool
    scaling: float
    center_freq: float
    trigger_timestamp: int
    input_select: int
    average_count: int
    num_segments: int
    num_total_segments: int
    first_segment_index: int
    num_missed_triggers: int

    @staticmethod
    def from_binary(
        binary: bytes,
        *,
        version: _HeaderVersion,
    ) -> ShfScopeVectorExtraHeader:
        """Parse the extra header of scope vectors.

        Copy of CoreShfNodeData.cpp

        Args:
            binary: The binary string representing the extra header.
            version: The version of the extra header.

        Returns:
            The parsed extra header.

        Raises:
            NotImplementedError: If the version is not supported.
        """
        if version.minor >= 2:  # noqa: PLR2004
            return ShfScopeVectorExtraHeader(
                timestamp=struct.unpack("q", binary[0:8])[0],
                timestamp_diff=struct.unpack("I", binary[8:12])[0],
                interleaved=struct.unpack("?", binary[15:16])[0],
                scaling=struct.unpack("d", binary[16:24])[0],
                center_freq=struct.unpack("d", binary[24:32])[0],
                trigger_timestamp=struct.unpack("q", binary[32:40])[0],
                input_select=struct.unpack("I", binary[40:44])[0],
                average_count=struct.unpack("I", binary[44:48])[0],
                num_segments=struct.unpack("I", binary[48:52])[0],
                num_total_segments=struct.unpack("I", binary[52:56])[0],
                first_segment_index=struct.unpack("I", binary[56:60])[0],
                num_missed_triggers=struct.unpack("I", binary[60:64])[0],
            )
        if version.minor >= 1:
            return ShfScopeVectorExtraHeader(
                timestamp=struct.unpack("q", binary[0:8])[0],
                timestamp_diff=struct.unpack("I", binary[8:12])[0],
                interleaved=struct.unpack("?", binary[15:16])[0],
                scaling=struct.unpack("d", binary[16:24])[0],
                center_freq=struct.unpack("d", binary[24:32])[0],
                trigger_timestamp=struct.unpack("q", binary[32:40])[0],
                input_select=struct.unpack("I", binary[40:44])[0],
                average_count=struct.unpack("I", binary[44:48])[0],
                num_segments=-1,
                num_total_segments=-1,
                first_segment_index=-1,
                num_missed_triggers=-1,
            )
        msg = str(
            f"Unsupported extra header version: {version} for "
            "ShfDemodulatorVectorExtraHeader",
        )
        raise NotImplementedError(msg)


@dataclass
class ShfDemodulatorVectorExtraHeader:
    """Class for the extra header of demodulator sample vectors.

    Args:
        timestamp: Timestamp of the first sample of the current block.
            (unit: number of 4GHz clock ticks)
        timestamp_diff: Timestamp delta between samples
            (unit: number of clock ticks on the max. sampling rate,
            e.g. 50MHz for demod samples)
        abort_config: Flag if a configuration change has taken place
        trigger_source: Index of the trigger used for this acquisition.
        trigger_length: Total number of samples associated with this trigger
        trigger_index: Index of the first sample in this block within the total
            sequence of triggered samples
        trigger_tag: Trigger number that activated the recording of this
            sequence of samples
        awg_tag: AWG tag present when the trigger was activated
        scaling: Scaling value to convert X/Y from fixed point two's
            complement to Volts
        center_freq: Configured center frequency
        oscillator_source: 	Index of the oscillator used for this acquisition.
        signal_source: Index of the signal input used for this acquisition.
    """

    timestamp: int
    timestamp_diff: int
    abort_config: bool
    trigger_source: int
    trigger_length: int
    trigger_index: int
    trigger_tag: int
    awg_tag: int
    scaling: float
    center_freq: float
    oscillator_source: int
    signal_source: int

    @staticmethod
    def from_binary(
        binary: bytes,
        *,
        version: _HeaderVersion,
    ) -> ShfDemodulatorVectorExtraHeader:
        """Parse the extra header of demodulator vectors.

        Copy of CoreShfNodeData.cpp

        Args:
            binary: The binary string representing the extra header.
            version: The version of the extra header.

        Returns:
            The parsed extra header.

        Raises:
            NotImplementedError: If the version is not supported.
        """
        # To be correct, these values should be read from
        # /dev.../system/properties/timebase and
        # /dev.../system/properties/maxdemodrate
        # Here we have read them once and hardcoded for simplicity
        timebase = 2.5e-10
        max_demod_rate = 5e7
        if version.minor >= 2:  # noqa: PLR2004
            timestamp_diff = struct.unpack("I", binary[8:12])[0]
            timestamp_diff *= 1 / (timebase * max_demod_rate)
            return ShfDemodulatorVectorExtraHeader(
                timestamp=struct.unpack("q", binary[0:8])[0],
                timestamp_diff=timestamp_diff,
                abort_config=struct.unpack("?", binary[12:13])[0],
                trigger_source=struct.unpack("B", binary[13:14])[0],
                trigger_length=struct.unpack("I", binary[16:20])[0],
                trigger_index=struct.unpack("I", binary[20:24])[0],
                trigger_tag=struct.unpack("I", binary[24:28])[0],
                awg_tag=struct.unpack("I", binary[28:32])[0],
                scaling=struct.unpack("d", binary[32:40])[0],
                center_freq=struct.unpack("d", binary[40:48])[0],
                oscillator_source=struct.unpack("H", binary[48:50])[0],
                signal_source=struct.unpack("H", binary[50:52])[0],
            )
        if version.minor >= 1:
            timestamp_diff = struct.unpack("I", binary[8:12])[0]
            timestamp_diff *= 1 / (timebase * max_demod_rate)
            return ShfDemodulatorVectorExtraHeader(
                timestamp=struct.unpack("q", binary[0:8])[0],
                timestamp_diff=timestamp_diff,
                abort_config=struct.unpack("?", binary[12:13])[0],
                trigger_source=struct.unpack("B", binary[13:14])[0],
                trigger_length=struct.unpack("I", binary[16:20])[0],
                trigger_index=struct.unpack("I", binary[20:24])[0],
                trigger_tag=struct.unpack("I", binary[24:28])[0],
                awg_tag=struct.unpack("I", binary[28:32])[0],
                scaling=struct.unpack("d", binary[32:40])[0],
                center_freq=struct.unpack("d", binary[40:48])[0],
                oscillator_source=-1,
                signal_source=-1,
            )
        msg = str(
            f"Unsupported extra header version: {version} for "
            "ShfDemodulatorVectorExtraHeader",
        )
        raise NotImplementedError(msg)


ExtraHeader = Union[
    ShfScopeVectorExtraHeader,
    ShfDemodulatorVectorExtraHeader,
    ShfResultLoggerVectorExtraHeader,
]


def _parse_extra_header_version(extra_header_info: int) -> _HeaderVersion:
    """Extract the header version from the extra header info.

    Copy of CoreVectorNodeData.cpp

    Args:
        extra_header_info: The extra header info.

    Returns:
        The header version.
    """
    if extra_header_info == 0:
        msg = "Vector data does not contain extra header."
        raise ValueError(msg)
    version = extra_header_info >> 16
    return _HeaderVersion(major=(version & 0xE0) >> 5, minor=version & 0x1F)


def _deserialize_shf_waveform_vector(
    raw_data: bytes,
) -> np.ndarray:
    """Deserialize the vector data for waveform vectors.

    Copy of CoreShfNodeData.cpp

    Args:
        raw_data: The binary data representing the vector.

    Returns:
        The deserialized vector and the extra header (None for waveforms).
    """
    shf_wavforms_signed_encoding_bits = 17
    scaling = 1 / float((1 << shf_wavforms_signed_encoding_bits) - 1)

    data = np.frombuffer(raw_data, dtype=np.int32) * scaling
    data_real = data[::2]
    data_imag = data[1::2]
    return data_real + 1j * data_imag


def _deserialize_shf_result_logger_vector(
    *,
    raw_data: bytes,
    extra_header_info: int,
    header_length: int,
    element_type: VectorElementType,
) -> tuple[np.ndarray, ShfResultLoggerVectorExtraHeader]:
    """Deserialize the vector data for result logger vector.

    Args:
        raw_data: The binary data representing the vector.
        extra_header_info: The extra header info for the vector.
        header_length: The length of the extra header of the vector.
        element_type: Type of the elements in the vector.

    Returns:
        The deserialized vector and the extra header
    """
    # Parse header
    raw_extra_header = raw_data[:header_length]
    version = _parse_extra_header_version(extra_header_info)
    extra_header = ShfResultLoggerVectorExtraHeader.from_binary(
        raw_extra_header,
        version=version,
    )

    # Parse raw data
    data = np.frombuffer(
        raw_data[header_length:],
        dtype=element_type.to_numpy_type(),
    )
    return data, extra_header


def _deserialize_shf_scope_vector(
    *,
    raw_data: bytes,
    extra_header_info: int,
    header_length: int,
) -> tuple[np.ndarray, ShfScopeVectorExtraHeader]:
    """Deserialize the vector data for waveform vectors.

    Args:
        raw_data: The binary data representing the vector.
        extra_header_info: The extra header info for the vector.
        header_length: The length of the extra header of the vector.

    Returns:
        The deserialized vector and the extra header
    """
    # Parse header
    raw_extra_header = raw_data[:header_length]
    version = _parse_extra_header_version(extra_header_info)
    extra_header = ShfScopeVectorExtraHeader.from_binary(
        raw_extra_header,
        version=version,
    )

    # Parse raw data
    data = (
        np.frombuffer(raw_data[header_length:], dtype=np.int32) * extra_header.scaling
    )
    data_real = data[::2]
    data_imag = data[1::2]
    return data_real + 1j * data_imag, extra_header


def _deserialize_shf_demodulator_vector(
    *,
    raw_data: bytes,
    extra_header_info: int,
    header_length: int,
) -> tuple[SHFDemodSample, ShfDemodulatorVectorExtraHeader]:
    """Deserialize the vector data for waveform vectors.

    Args:
        raw_data: The binary data representing the vector.
        extra_header_info: The extra header info for the vector.
        header_length: The length of the extra header of the vector.

    Returns:
        The deserialized vector and the extra header
    """
    # Parse header
    raw_extra_header = raw_data[:header_length]
    version = _parse_extra_header_version(extra_header_info)
    extra_header = ShfDemodulatorVectorExtraHeader.from_binary(
        raw_extra_header,
        version=version,
    )

    # Parse raw data
    data = np.frombuffer(raw_data[header_length:], np.int64) * extra_header.scaling
    data_x = data[::2]
    data_y = data[1::2]
    return SHFDemodSample(data_x, data_y), extra_header


def get_header_length(vector_data: CapnpCapability) -> int:
    """Get the length of the extra header.

    The 16 least significant bits of extra_header_info contain the length of
    the header, expressed in 32-bits words. Take the 16 lsb with & 0x0000FFFF,
    then multiply by 4 with << 2 to express the length in bytes.

    Args:
        vector_data: The vector data struct.

    Returns:
        The length of the extra header in bytes.
    """
    return (vector_data.extraHeaderInfo & 0x0000FFFF) << 2


def parse_shf_vector_data_struct(
    vector_data: CapnpCapability,
) -> tuple[np.ndarray | SHFDemodSample, ExtraHeader | None]:
    """Parse the SHF vector data struct.

    An SHF vector consists of an extra header and a the data vector.

    Args:
        vector_data: The vector data struct.

    Returns:
        The deserialized vector and the extra header

    Raises:
        ValueError: If the vector value type is not supported.
    """
    raw_data = vector_data.data
    extra_header_info: int = vector_data.extraHeaderInfo
    header_length = get_header_length(vector_data)

    value_type = vector_data.valueType
    if value_type == VectorValueType.SHF_SCOPE_VECTOR_DATA:
        return _deserialize_shf_scope_vector(
            raw_data=raw_data,
            extra_header_info=extra_header_info,
            header_length=header_length,
        )
    if value_type == VectorValueType.SHF_DEMODULATOR_VECTOR_DATA:
        return _deserialize_shf_demodulator_vector(
            raw_data=raw_data,
            extra_header_info=extra_header_info,
            header_length=header_length,
        )
    if value_type == VectorValueType.SHF_RESULT_LOGGER_VECTOR_DATA:
        return _deserialize_shf_result_logger_vector(
            raw_data=raw_data,
            extra_header_info=extra_header_info,
            header_length=header_length,
            element_type=VectorElementType(vector_data.vectorElementType),
        )
    if value_type == VectorValueType.SHF_GENERATOR_WAVEFORM_VECTOR_DATA:
        return _deserialize_shf_waveform_vector(raw_data), None
    msg = f"Unsupported vector value type: {value_type}"
    raise ValueError(msg)
