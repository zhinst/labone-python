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

import logging
import struct
import typing as t
from dataclasses import dataclass
from typing import Union

import numpy as np

from labone.core.errors import LabOneCoreError, SHFHeaderVersionNotSupportedError
from labone.core.helper import VectorElementType, VectorValueType

if t.TYPE_CHECKING:
    from zhinst.comms import DynamicStruct


logger = logging.getLogger(__name__)

SHF_WAVEFORM_UNSIGNED_ENCODING_BITS = 18
SHF_WAVEFORM_SIGNED_ENCODING_BITS = SHF_WAVEFORM_UNSIGNED_ENCODING_BITS - 1
SHF_WAVEFORM_SCALING = (1 << SHF_WAVEFORM_SIGNED_ENCODING_BITS) - 1


@dataclass(frozen=True)
class _HeaderVersion:
    """Class for the version of the extra header."""

    major: int
    minor: int

    def as_tuple(self) -> tuple[int, int]:
        """Return the version as tuple."""
        return self.major, self.minor


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
        job_id: Job ID of the current measurement
        repetition_id: Repetition ID of the current measurement
        scaling: Scaling value to convert the measurement data from 32-bit
            signed integer to double
        center_freq: configured center frequency
        data_source: Data source used for this measurement
        samples: Number of samples per shot
        spectr_samples: Number of samples per shot in the spectrum
        averages: Number of averages per shot
        acquired: Number of shots acquired
        holdoff_errors_reslog: Number of hold-off errors in the result logger
        holdoff_errors_readout: Number of hold-off errors in the readout
        holdoff_errors_spectr: Number of hold-off errors in the spectrum
        first_sample_timestamp: Timestamp of the first sample
    """

    timestamp: int
    job_id: int
    repetition_id: int
    scaling: float
    center_freq: float
    data_source: int
    samples: int
    spectr_samples: int
    averages: int
    acquired: int
    holdoff_errors_reslog: int
    holdoff_errors_readout: int
    holdoff_errors_spectr: int
    first_sample_timestamp: int

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
            SHFHeaderVersionNotSupportedError: If the version is not supported.
        """
        if (version.major == 0) and (version.minor >= 1):
            return ShfResultLoggerVectorExtraHeader(
                timestamp=struct.unpack("q", binary[0:8])[0],
                job_id=struct.unpack("I", binary[8:12])[0],
                repetition_id=struct.unpack("I", binary[12:16])[0],
                scaling=struct.unpack("d", binary[16:24])[0],
                center_freq=struct.unpack("d", binary[24:32])[0],
                data_source=struct.unpack("I", binary[32:36])[0],
                samples=struct.unpack("I", binary[36:40])[0],
                spectr_samples=struct.unpack("I", binary[40:44])[0],
                averages=struct.unpack("I", binary[44:48])[0],
                acquired=struct.unpack("I", binary[48:52])[0],
                holdoff_errors_reslog=struct.unpack("H", binary[52:54])[0],
                holdoff_errors_readout=struct.unpack("H", binary[54:56])[0],
                holdoff_errors_spectr=struct.unpack("H", binary[56:58])[0],
                first_sample_timestamp=struct.unpack("q", binary[58:66])[0],
            )
        raise SHFHeaderVersionNotSupportedError(version=version.as_tuple())

    def to_binary(self) -> tuple[bytes, _HeaderVersion]:
        """Pack the extra header into a binary string.

        Returns:
            The binary string representing the extra header
            and the version of the extra header used for
            this encoding.
        """
        return struct.pack(
            "=qIIddIIIIIHHHqH",
            self.timestamp,
            self.job_id,
            self.repetition_id,
            self.scaling,
            self.center_freq,
            self.data_source,
            self.samples,
            self.spectr_samples,
            self.averages,
            self.acquired,
            self.holdoff_errors_reslog,
            self.holdoff_errors_readout,
            self.holdoff_errors_spectr,
            self.first_sample_timestamp,
            0,  # padding to make the number of bytes divisible by 4
            # this is necessary because the length of the header is encoded
            # in multiples of 4 bytes (32 bit words)
        ), _HeaderVersion(major=0, minor=2)


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
        segments: 	Number of segments contained in the scope vector
        total_segments: Total number of segments. This allows sending
            partial results by setting segments < total_segments
        first_segment_index: Index of the first segment in the scope vector
        missed_triggers: Number of missed triggers due to hold-off time
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
    segments: int
    total_segments: int
    first_segment_index: int
    missed_triggers: int

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
            SHFHeaderVersionNotSupportedError: If the version is not supported.
        """
        if (version.major == 0) and (version.minor >= 2):  # noqa: PLR2004
            return ShfScopeVectorExtraHeader(
                timestamp=struct.unpack("q", binary[0:8])[0],
                timestamp_diff=struct.unpack("I", binary[8:12])[0],
                interleaved=struct.unpack("?", binary[15:16])[0],
                scaling=struct.unpack("d", binary[16:24])[0],
                center_freq=struct.unpack("d", binary[24:32])[0],
                trigger_timestamp=struct.unpack("q", binary[32:40])[0],
                input_select=struct.unpack("I", binary[40:44])[0],
                average_count=struct.unpack("I", binary[44:48])[0],
                segments=struct.unpack("I", binary[48:52])[0],
                total_segments=struct.unpack("I", binary[52:56])[0],
                first_segment_index=struct.unpack("I", binary[56:60])[0],
                missed_triggers=struct.unpack("I", binary[60:64])[0],
            )
        if (version.major == 0) and (version.minor >= 1):
            return ShfScopeVectorExtraHeader(
                timestamp=struct.unpack("q", binary[0:8])[0],
                timestamp_diff=struct.unpack("I", binary[8:12])[0],
                interleaved=struct.unpack("?", binary[15:16])[0],
                scaling=struct.unpack("d", binary[16:24])[0],
                center_freq=struct.unpack("d", binary[24:32])[0],
                trigger_timestamp=struct.unpack("q", binary[32:40])[0],
                input_select=struct.unpack("I", binary[40:44])[0],
                average_count=struct.unpack("I", binary[44:48])[0],
                segments=-1,
                total_segments=-1,
                first_segment_index=-1,
                missed_triggers=-1,
            )
        raise SHFHeaderVersionNotSupportedError(version=version.as_tuple())

    def to_binary(self) -> tuple[bytes, _HeaderVersion]:
        """Pack the extra header into a binary string.

        Returns:
            The binary string representing the extra header
            and the version of the extra header used for
            this encoding.
        """
        return struct.pack(
            "qIBddqIIIIII",
            self.timestamp,
            self.timestamp_diff,
            self.interleaved,
            self.scaling,
            self.center_freq,
            self.trigger_timestamp,
            self.input_select,
            self.average_count,
            self.segments,
            self.total_segments,
            self.first_segment_index,
            self.missed_triggers,
        ), _HeaderVersion(major=0, minor=2)


@dataclass
class ShfDemodulatorVectorExtraHeader:
    """Class for the extra header of demodulator sample vectors.

    Args:
        timestamp: Timestamp of the first sample of the current block.
            (unit: number of 4GHz clock ticks)
        timestamp_delta: Timestamp delta between samples
            (unit: number of clock ticks on the max. sampling rate,
            e.g. 50MHz for demod samples)
        burst_length: Length of the burst in samples
        burst_offset: Index of the first sample in this block within the total
            sequence of the burst
        trigger_index: Trigger counter (including missed triggers). Each vector
            contains actual counter value.
        trigger_timestamp: Timestamp of the moment when a trigger happened.
        center_freq: Center frequency
        rf_path: Flag that indicates if RF-path is selected
        oscillator_source: Index of the oscillator used for this acquisition.
        harmonic: Harmonic of the oscillator used for this acquisition.
        trigger_source: Index of the trigger source used for this acquisition.
        signal_source: Index of the signal input used for this acquisition.
        oscillator_freq: Current oscillator frequency.
        scaling: Scaling value to convert the measurement data from 32-bit
            signed integer to double.
    """

    timestamp: int
    timestamp_delta: int
    burst_length: int
    burst_offset: int
    trigger_index: int
    trigger_timestamp: int
    center_freq: int
    rf_path: bool
    oscillator_source: int
    harmonic: int
    trigger_source: int
    signal_source: int
    oscillator_freq: int
    scaling: float

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
            SHFHeaderVersionNotSupportedError: If the version is not supported.
        """
        # To be correct, these values should be read from
        # /dev.../system/properties/timebase and
        # /dev.../system/properties/maxdemodrate
        # Here we have read them once and hardcoded for simplicity
        if version.major == 1:
            timestamp_delta = struct.unpack("I", binary[8:12])[0]
            source_field = struct.unpack("I", binary[36:40])[0]
            return ShfDemodulatorVectorExtraHeader(
                timestamp=struct.unpack("q", binary[0:8])[0],
                timestamp_delta=timestamp_delta,
                burst_length=struct.unpack("I", binary[12:16])[0],
                burst_offset=struct.unpack("I", binary[16:20])[0],
                trigger_index=struct.unpack("I", binary[20:24])[0],
                trigger_timestamp=struct.unpack("q", binary[24:32])[0],
                center_freq=struct.unpack("h", binary[32:34])[0],
                rf_path=struct.unpack("?", binary[34:35])[0],
                oscillator_source=source_field & 0b111,
                harmonic=(source_field >> 3) & 0b1111111111,
                trigger_source=(source_field >> 13) & 0b111111,
                signal_source=(source_field >> 19) & 0b111111,
                oscillator_freq=struct.unpack("q", binary[40:48])[0],
                scaling=struct.unpack("f", binary[48:52])[0],
            )
        raise SHFHeaderVersionNotSupportedError(version=version.as_tuple())

    def to_binary(self) -> tuple[bytes, _HeaderVersion]:
        """Pack the extra header into a binary string.

        Returns:
            The binary string representing the extra header
            and the version of the extra header used for
            this encoding.
        """
        source_field = (
            (self.signal_source << 19)
            | (self.trigger_source << 13)
            | (self.harmonic << 3)
            | self.oscillator_source
        )
        return struct.pack(
            "qIIIIqhBIqf",
            self.timestamp,
            self.timestamp_delta,
            self.burst_length,
            self.burst_offset,
            self.trigger_index,
            self.trigger_timestamp,
            self.center_freq,
            self.rf_path,
            source_field,
            self.oscillator_freq,
            self.scaling,
        ), _HeaderVersion(major=1, minor=0)


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
) -> tuple[np.ndarray, ShfResultLoggerVectorExtraHeader | None]:
    """Deserialize the vector data for result logger vector.

    Args:
        raw_data: The binary data representing the vector.
        extra_header_info: The extra header info for the vector.
        header_length: The length of the extra header of the vector.
        element_type: Type of the elements in the vector.

    Returns:
        The deserialized vector and the extra header

    Raises:
        SHFHeaderVersionNotSupportedError: If the version is not supported.
        LabOneCoreError: If the version cannot be parsed.
    """
    # Parse header
    raw_extra_header = raw_data[:header_length]
    try:
        version = _parse_extra_header_version(extra_header_info)
    except ValueError as e:
        if len(raw_data) == 0:
            return np.array([], dtype=np.int32), None
        msg = (
            "Unable to parse the version of the shf result vector."  # pragma: no cover
        )
        raise LabOneCoreError(msg) from e  # pragma: no cover
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
) -> tuple[np.ndarray, ShfScopeVectorExtraHeader | None]:
    """Deserialize the vector data for waveform vectors.

    Args:
        raw_data: The binary data representing the vector.
        extra_header_info: The extra header info for the vector.
        header_length: The length of the extra header of the vector.

    Returns:
        The deserialized vector and the extra header

    Raises:
        SHFHeaderVersionNotSupportedError: If the version is not supported.
        LabOneCoreError: If the version cannot be parsed.
    """
    # Parse header
    raw_extra_header = raw_data[:header_length]
    try:
        version = _parse_extra_header_version(extra_header_info)
    except ValueError as e:
        if len(raw_data) == 0:
            return np.array([], dtype=np.int32), None
        msg = "Unable to parse the version of the shf scope vector."  # pragma: no cover
        raise LabOneCoreError(msg) from e  # pragma: no cover

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
) -> tuple[SHFDemodSample, ShfDemodulatorVectorExtraHeader | None]:
    """Deserialize the vector data for waveform vectors.

    Args:
        raw_data: The binary data representing the vector.
        extra_header_info: The extra header info for the vector.
        header_length: The length of the extra header of the vector.

    Returns:
        The deserialized vector and the extra header

    Raises:
        SHFHeaderVersionNotSupportedError: If the version is not supported.
        LabOneCoreError: If the version cannot be parsed.
    """
    # Parse header
    raw_extra_header = raw_data[:header_length]
    try:
        version = _parse_extra_header_version(extra_header_info)
    except ValueError as e:
        if len(raw_data) == 0:
            return (
                SHFDemodSample(
                    x=np.array([], dtype=np.int32),
                    y=np.array([], dtype=np.int32),
                ),
                None,
            )
        msg = "Unable to parse the version of the shf demod vector."  # pragma: no cover
        raise LabOneCoreError(msg) from e  # pragma: no cover
    extra_header = ShfDemodulatorVectorExtraHeader.from_binary(
        raw_extra_header,
        version=version,
    )

    # Parse raw data
    data = np.frombuffer(raw_data[header_length:], np.int64) * extra_header.scaling
    data_x = data[::2]
    data_y = data[1::2]
    return SHFDemodSample(data_x, data_y), extra_header


def get_header_length(vector_data: DynamicStruct) -> int:
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
    vector_data: DynamicStruct,
) -> tuple[np.ndarray | SHFDemodSample, ExtraHeader | None]:
    """Parse the SHF vector data struct.

    An SHF vector consists of an extra header and a the data vector.

    Args:
        vector_data: The vector data struct.

    Returns:
        The deserialized vector and the extra header

    Raises:
        ValueError: If the vector value type is not supported.
        SHFHeaderVersionNotSupportedError: If the version is not supported.
        LabOneCoreError: If the version cannot be parsed.
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


def encode_shf_vector_data_struct(
    *,
    data: np.ndarray | SHFDemodSample,
    extra_header: ExtraHeader,
) -> dict[str, t.Any]:
    """Encode the SHF vector data struct.

    Build a struct (in form of a dictionary) from data and
    extra header to send.

    Args:
        data: The vector data.
        extra_header: The extra header.

    Returns:
        The struct to send.
    """
    if isinstance(extra_header, ShfScopeVectorExtraHeader):
        if not isinstance(data, np.ndarray):
            msg = "data must be of type np.ndarray for ShfScopeVectorExtraHeader"
            raise TypeError(msg)
        value_type = VectorValueType.SHF_SCOPE_VECTOR_DATA

        # actually, this should be type int32, but signed int32 not supported
        # by the LabOne type enum. Using unsigned uint32 for transmission.
        # (This ensures the same behaviour than in LabOne)
        vector_element_type_np: t.Any = np.uint32

        data /= extra_header.scaling

        # bring into format [1_real, 1_imag, 2_real, 2_imag, ...]
        # all values as int
        data_to_send_np = np.empty((2 * data.size,), dtype=np.int32)
        data_to_send_np[0::2] = data.real
        data_to_send_np[1::2] = data.imag

    elif isinstance(extra_header, ShfDemodulatorVectorExtraHeader):
        if not isinstance(data, SHFDemodSample):
            msg = (
                "data must be of type SHFDemodSample "
                "for ShfDemodulatorVectorExtraHeader"
            )
            raise TypeError(msg)

        value_type = VectorValueType.SHF_DEMODULATOR_VECTOR_DATA

        # actually, this should be type int64, but signed int64 not supported
        # by the type enum. Using unsigned uint64 for transmission.
        # (This ensures the same behaviour than in LabOne)
        vector_element_type_np = np.uint64

        data.x = np.array(data.x / extra_header.scaling, dtype=np.int64)
        data.y = np.array(data.y / extra_header.scaling, dtype=np.int64)

        data_to_send_np = np.empty((2 * data.x.size,), dtype=np.int64)
        data_to_send_np[0::2] = data.x
        data_to_send_np[1::2] = data.y

    else:
        # extra_header is a ShfResultLoggerVectorExtraHeader
        if not isinstance(data, np.ndarray):
            msg = "data must be of type np.ndarray for ShfResultLoggerVectorExtraHeader"
            raise TypeError(msg)

        value_type = VectorValueType.SHF_RESULT_LOGGER_VECTOR_DATA
        vector_element_type_np = data.dtype
        data_to_send_np = data

    extra_header_bytes, version = extra_header.to_binary()

    # >> 2 to get 32 bit words from bytes
    len_extra_header: int = len(extra_header_bytes) >> 2

    # encoding will be incorrect, if len_extra_header is >= 2^16
    # or minor >= 2^3 or major >= 2^5
    extra_header_info: int = (
        version.major << (5 + 16) | version.minor << 16 | len_extra_header
    )

    return {
        "valueType": value_type.value,
        "vectorElementType": VectorElementType.from_numpy_type(
            vector_element_type_np,
        ).value,
        "extraHeaderInfo": extra_header_info,
        "data": extra_header_bytes + data_to_send_np.tobytes(),
    }


def preprocess_complex_shf_waveform_vector(
    data: np.ndarray,
) -> tuple[np.ndarray, t.Any]:
    """Preprocess complex waveform vector data.

    Complex waveform vectors are transmitted as two uint32 interleaved vectors.
    This function converts the complex waveform vector data into the
    corresponding uint32 vector.

    Args:
        data: The complex waveform vector data.

    Returns:
        The uint32 vector data.
    """
    real_scaled = np.round(np.real(data) * SHF_WAVEFORM_SCALING).astype(np.int32)
    imag_scaled = np.round(np.imag(data) * SHF_WAVEFORM_SCALING).astype(np.int32)
    decoded_data = np.empty((2 * data.size,), dtype=np.int32)
    decoded_data[::2] = real_scaled
    decoded_data[1::2] = imag_scaled

    return decoded_data, np.uint32
