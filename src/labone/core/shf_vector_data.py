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
from dataclasses import dataclass

import numpy as np

from labone.core import hpk_schema
from labone.core.errors import LabOneCoreError, SHFHeaderVersionNotSupportedError
from labone.core.helper import VectorElementType, VectorValueType

logger = logging.getLogger(__name__)

_SHF_WAVEFORM_UNSIGNED_ENCODING_BITS = 18
_SHF_WAVEFORM_SIGNED_ENCODING_BITS = _SHF_WAVEFORM_UNSIGNED_ENCODING_BITS - 1
_SHF_WAVEFORM_SCALING = (1 << _SHF_WAVEFORM_SIGNED_ENCODING_BITS) - 1
_SUPPORTED_SHF_VECTOR_DATA_TYPES = [
    VectorValueType.SHF_GENERATOR_WAVEFORM_VECTOR_DATA,
    VectorValueType.SHF_RESULT_LOGGER_VECTOR_DATA,
    VectorValueType.SHF_SCOPE_VECTOR_DATA,
]


@dataclass(frozen=True)
class _HeaderVersion:
    """Class for the version of the extra header."""

    major: int
    minor: int

    def as_tuple(self) -> tuple[int, int]:
        """Return the version as tuple."""
        return self.major, self.minor


@dataclass
class ShfResultLoggerVectorData:
    """SHF result logger sample data."""

    vector: np.ndarray
    properties: hpk_schema.ShfResultLoggerVectorDataProperties


def shf_result_logger_properties_from_binary(
    binary: bytes,
    *,
    version: _HeaderVersion,
) -> hpk_schema.ShfResultLoggerVectorDataProperties:
    """Parse the extra header of result logger vectors.

    Args:
        binary: The binary string representing the extra header.
        version: The version of the extra header.

    Returns:
        The parsed extra header.

    Raises:
        SHFHeaderVersionNotSupportedError: If the version is not supported.
    """
    if (version.major == 0) and (version.minor >= 1):
        result = hpk_schema.ShfResultLoggerVectorData()
        prop = result.init_properties()
        prop.timestamp = struct.unpack("q", binary[0:8])[0]
        prop.jobId = struct.unpack("I", binary[8:12])[0]
        prop.repetitionId = struct.unpack("I", binary[12:16])[0]
        prop.scaling = struct.unpack("d", binary[16:24])[0]
        prop.centerFrequency = struct.unpack("d", binary[24:32])[0]
        prop.dataSource = struct.unpack("I", binary[32:36])[0]
        prop.numSamples = struct.unpack("I", binary[36:40])[0]
        prop.numSpectrSamples = struct.unpack("I", binary[40:44])[0]
        prop.numAverages = struct.unpack("I", binary[44:48])[0]
        prop.numAcquired = struct.unpack("I", binary[48:52])[0]
        prop.holdoffErrorsReslog = struct.unpack("H", binary[52:54])[0]
        prop.holdoffErrorsReadout = struct.unpack("H", binary[54:56])[0]
        prop.holdoffErrorsSpectr = struct.unpack("H", binary[56:58])[0]
        prop.firstSampleTimestamp = struct.unpack("Q", binary[58:66])[0]
        return result.properties
    raise SHFHeaderVersionNotSupportedError(version=version.as_tuple())


@dataclass
class ShfScopeVectorData:
    """SHF scope sample data."""

    vector: np.ndarray
    properties: hpk_schema.ShfScopeVectorDataProperties


def shf_scope_properties_from_binary(
    binary: bytes,
    *,
    version: _HeaderVersion,
) -> hpk_schema.ShfScopeVectorDataProperties:
    """Parse the extra header of scope vectors.

    Args:
        binary: The binary string representing the extra header.
        version: The version of the extra header.

    Returns:
        The parsed extra header.

    Raises:
        SHFHeaderVersionNotSupportedError: If the version is not supported.
    """
    if (version.major == 0) and (version.minor >= 2):  # noqa: PLR2004
        result = hpk_schema.ShfScopeVectorData()
        prop = result.init_properties()
        prop.timestamp = struct.unpack("q", binary[0:8])[0]
        prop.timestampDiff = struct.unpack("I", binary[8:12])[0]
        prop.flags = struct.unpack("I", binary[12:16])[0]
        prop.scaling = struct.unpack("d", binary[16:24])[0]
        prop.centerFrequency = struct.unpack("d", binary[24:32])[0]
        prop.triggerTimestamp = struct.unpack("q", binary[32:40])[0]
        prop.inputSelect = struct.unpack("I", binary[40:44])[0]
        prop.averageCount = struct.unpack("I", binary[44:48])[0]
        prop.numSegments = struct.unpack("I", binary[48:52])[0]
        prop.numTotalSegments = struct.unpack("I", binary[52:56])[0]
        prop.firstSegmentIndex = struct.unpack("I", binary[56:60])[0]
        prop.numMissedTriggers = struct.unpack("I", binary[60:64])[0]
        return result.properties
    if (version.major == 0) and (version.minor >= 1):
        result = hpk_schema.ShfScopeVectorData()
        prop = result.init_properties()
        prop.timestamp = struct.unpack("q", binary[0:8])[0]
        prop.timestampDiff = struct.unpack("I", binary[8:12])[0]
        prop.flags = struct.unpack("I", binary[12:16])[0]
        prop.scaling = struct.unpack("d", binary[16:24])[0]
        prop.centerFrequency = struct.unpack("d", binary[24:32])[0]
        prop.triggerTimestamp = struct.unpack("q", binary[32:40])[0]
        prop.inputSelect = struct.unpack("I", binary[40:44])[0]
        prop.averageCount = struct.unpack("I", binary[44:48])[0]
        return result.properties
    raise SHFHeaderVersionNotSupportedError(version=version.as_tuple())


@dataclass
class ShfDemodulatorVectorData:
    """SHF demodulator sample data."""

    x: np.ndarray
    y: np.ndarray
    properties: hpk_schema.ShfDemodulatorVectorDataProperties


def _parse_extra_header_version(extra_header_info: int) -> _HeaderVersion:
    """Extract the header version from the extra header info.

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


def _deserialize_shf_result_logger_vector(
    *,
    raw_data: bytes,
    extra_header_info: int,
    header_length: int,
    element_type: VectorElementType,
) -> ShfResultLoggerVectorData:
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
            return ShfResultLoggerVectorData(
                vector=np.array([], dtype=np.int32),
                properties=hpk_schema.ShfResultLoggerVectorData().properties,
            )
        msg = (
            "Unable to parse the version of the shf result vector."  # pragma: no cover
        )
        raise LabOneCoreError(msg) from e  # pragma: no cover
    extra_header = shf_result_logger_properties_from_binary(
        raw_extra_header,
        version=version,
    )

    # Parse raw data
    data = np.frombuffer(
        raw_data[header_length:],
        dtype=element_type.to_numpy_type(),
    )
    return ShfResultLoggerVectorData(vector=data, properties=extra_header)


def _deserialize_shf_scope_vector(
    *,
    raw_data: bytes,
    extra_header_info: int,
    header_length: int,
) -> ShfScopeVectorData:
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
            return ShfScopeVectorData(
                vector=np.array([], dtype=np.int32),
                properties=hpk_schema.ShfScopeVectorData().properties,
            )
        msg = "Unable to parse the version of the shf scope vector."  # pragma: no cover
        raise LabOneCoreError(msg) from e  # pragma: no cover

    extra_header = shf_scope_properties_from_binary(
        raw_extra_header,
        version=version,
    )

    # Parse raw data
    data = (
        np.frombuffer(raw_data[header_length:], dtype=np.int32) * extra_header.scaling
    )
    data_real = data[::2]
    data_imag = data[1::2]
    return ShfScopeVectorData(
        vector=data_real + 1j * data_imag,
        properties=extra_header,
    )


@dataclass
class ShfGeneratorWaveformVectorData:
    """SHF generator waveform sample data."""

    complex: np.ndarray


def _deserialize_shf_waveform_vector(
    raw_data: bytes,
) -> ShfGeneratorWaveformVectorData:
    """Deserialize the vector data for waveform vectors.

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
    return ShfGeneratorWaveformVectorData(complex=data_real + 1j * data_imag)


@dataclass
class ShfPidVectorData:
    """SHF pid sample data."""

    value: np.ndarray
    error: np.ndarray
    properties: hpk_schema.ShfPidVectorDataProperties


def get_header_length(vector_data: hpk_schema.VectorData) -> int:
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


def supports_shf_vector_parsing_from_vector_data(value_type: int) -> bool:
    """Check if the value type is supported for SHF vector parsing.

    Not all value types are supported for SHF vector parsing. Since the latest
    version of LabOne supports parsing the SHF vectors on the server side, this
    is a deprecated function.

    Args:
        value_type: The value type to check.

    Returns:
        True if the value type is supported, False otherwise.
    """
    return value_type in _SUPPORTED_SHF_VECTOR_DATA_TYPES


def parse_shf_vector_from_vector_data(
    vector_data: hpk_schema.VectorData,
) -> ShfScopeVectorData | ShfResultLoggerVectorData | ShfGeneratorWaveformVectorData:
    """Parse the SHF vector data struct.

    An SHF vector consists of an extra header and a the data vector.
    Since the latest version of LabOne supports parsing the SHF vectors on the
    server side, this is a deprecated function.

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
    if value_type == VectorValueType.SHF_RESULT_LOGGER_VECTOR_DATA:
        return _deserialize_shf_result_logger_vector(
            raw_data=raw_data,
            extra_header_info=extra_header_info,
            header_length=header_length,
            element_type=VectorElementType(vector_data.vectorElementType),
        )
    if value_type == VectorValueType.SHF_GENERATOR_WAVEFORM_VECTOR_DATA:
        return _deserialize_shf_waveform_vector(raw_data)
    msg = f"Unsupported vector value type: {value_type}"
    raise ValueError(msg)


def preprocess_complex_shf_waveform_vector(
    data: np.ndarray,
) -> dict:
    """Preprocess complex waveform vector data.

    Complex waveform vectors are transmitted as two uint32 interleaved vectors.
    This function converts the complex waveform vector data into the
    corresponding uint32 vector.

    Args:
        data: The complex waveform vector data.

    Returns:
        The uint32 vector data.
    """
    real_scaled = np.round(np.real(data) * _SHF_WAVEFORM_SCALING).astype(np.int32)
    imag_scaled = np.round(np.imag(data) * _SHF_WAVEFORM_SCALING).astype(np.int32)
    decoded_data = np.empty((2 * data.size,), dtype=np.int32)
    decoded_data[::2] = real_scaled
    decoded_data[1::2] = imag_scaled

    return {
        "vectorData": {
            "valueType": VectorValueType.VECTOR_DATA.value,
            "extraHeaderInfo": 0,
            "vectorElementType": VectorElementType.UINT32.value,
            "data": decoded_data.tobytes(),
        },
    }
