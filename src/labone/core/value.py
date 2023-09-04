"""Module to handle the creation and conversion of values between capnp and python.

The relevant class is the dataclass `AnnotatedValue`. It is used as the main
data container for all values send to and received by the kernel/server.
It has both a function to convert a capnp message to a python object and vice
versa.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Union

import capnp
import numpy as np

from labone.core.resources import session_protocol_capnp  # type: ignore[attr-defined]
from labone.core.shf_vector_data import (
    ExtraHeader,
    SHFDemodSample,
    VectorValueType,
    get_header_length,
    parse_shf_vector_data_struct,
)

logger = logging.getLogger(__name__)


@dataclass
class AnnotatedValue:
    """Python representation of a node value.

    This class is used both for parsing received values from the server
    and for packing values to be send to the server.

    Note that in order to send data to the server only the `value` and `path`
    attributes are relevant. The other attributes are only used for parsing
    received data and will be ignored by the kernel/server.

    Args:
        value: Node Value.
        path: Absolute node path.
        timestamp: Timestamp (us since the last device reboot) at which the
            device sent the value. (Only relevant for received values.)
        extra_header: For some types of vector nodes, additional information
            regarding the data. None otherwise.
    """

    value: Value
    path: str
    timestamp: int | None = None
    extra_header: Any = None

    @staticmethod
    def from_capnp(raw: session_protocol_capnp.AnnotatedValue) -> AnnotatedValue:
        """Convert a capnp AnnotatedValue to a python AnnotatedValue.

        Args:
            raw: The capnp AnnotatedValue to convert

        Returns:
            The converted AnnotatedValue.
        """
        value, extra_header = _capnp_value_to_python_value(raw.value)
        return AnnotatedValue(
            value=value,
            timestamp=raw.metadata.timestamp,
            path=raw.metadata.path,
            extra_header=extra_header,
        )

    def to_capnp(self) -> session_protocol_capnp.AnnotatedValue:
        """Convert a python AnnotatedValue to a capnp AnnotatedValue.

        Warning:
            This method is not the inversion of `from_capnp`. It is only
            packs the relevant information that are parsed by the server
            for a set request into a capnp message!

        Returns:
            The capnp message containing the relevant information for a set
            request.

        Raises:
            TypeError: If the `path` attribute is not of type `str`.
            LabOneCoreError: If the data type of the value to be set is not supported.
        """
        message = session_protocol_capnp.AnnotatedValue.new_message()
        try:
            message.metadata.path = self.path
        except (AttributeError, TypeError, capnp.KjException) as error:
            field_type = message.metadata.schema.fields["path"].proto.slot.type.which()
            msg = f"`path` attribute must be of type {field_type}."
            raise TypeError(msg) from error
        message.value = _value_from_python_types(self.value)
        return message


@dataclass
class TriggerSample:
    """Single trigger sample.

    Args:
        timestamp: The timestamp at which the values have been measured
        sample_tick: The sample tick at which the values have been measured
        trigger: Trigger bits
        missed_triggers: Missed trigger bits
        awg_trigger: AWG trigger values at the time of trigger
        dio: DIO values at the time of trigger
        sequence_index: AWG sequencer index at the time of trigger
    """

    timestamp: int
    sample_tick: int
    trigger: int
    missed_triggers: int
    awg_trigger: int
    dio: int
    sequence_index: int

    @staticmethod
    def from_capnp(raw: session_protocol_capnp.TriggerSample) -> TriggerSample:
        """Convert a capnp TriggerSample to a python TriggerSample.

        Args:
            raw: The capnp TriggerSample to convert

        Returns:
            The converted TriggerSample.
        """
        return TriggerSample(
            timestamp=raw.timestamp,
            sample_tick=raw.sampleTick,
            trigger=raw.trigger,
            missed_triggers=raw.missedTriggers,
            awg_trigger=raw.awgTrigger,
            dio=raw.dio,
            sequence_index=raw.sequenceIndex,
        )


@dataclass
class CntSample:
    """Single counter sample.

    Args:
        timestamp: The timestamp at which the values have been measured.
        counter: Counter value
        trigger: Trigger bits
    """

    timestamp: int
    counter: int
    trigger: int

    @staticmethod
    def from_capnp(raw: session_protocol_capnp.CntSample) -> CntSample:
        """Convert a capnp CntSample to a python CntSample.

        Args:
            raw: The capnp CntSample to convert

        Returns:
        The converted CntSample.
        """
        return CntSample(
            timestamp=raw.timestamp,
            counter=raw.counter,
            trigger=raw.trigger,
        )


# All possible types of values that can be stored in a node.
Value = Union[
    int,
    float,
    str,
    complex,
    np.ndarray,
    SHFDemodSample,
    TriggerSample,
    CntSample,
    None,
]


def _capnp_vector_to_value(
    vector_data: session_protocol_capnp.VectorData,
) -> tuple[np.ndarray | SHFDemodSample, ExtraHeader | None]:
    """Parse a capnp vector to a numpy array.

    In addition to the numpy array the function also returns the extra header
    of the vector if present. Extra header information are only present for
    a selected set of shf vector types and contain additional information
    about the vector data.

    Args:
        vector_data: The capnp vector data to parse.

    Returns:
        Numpy array containing the vector data and the extra header if present.
    """
    raw_data = vector_data.data
    element_type = _VectorElementType(vector_data.vectorElementType)
    generic_vector_types = [VectorValueType.VECTOR_DATA, VectorValueType.BYTE_ARRAY]
    if vector_data.valueType not in generic_vector_types:
        # For the time being we need to manually untangle the shf vector types.
        # since it is planed to do this directly on the server side this logic
        # is outsourced in a different module.
        try:
            return parse_shf_vector_data_struct(vector_data)
        except ValueError:
            # Even though we are unable to parse the shf vector data we should
            # still return the data without the extra header info.
            logger.exception("Unknown shf vector type.")
            bytes_to_skip = get_header_length(vector_data)
            parse_vector = np.frombuffer(
                raw_data[bytes_to_skip:],
                dtype=element_type.to_numpy_type(),
            )
            return parse_vector, None

    if element_type == _VectorElementType.STRING:
        # Special case for strings which are send as byte arrays
        return raw_data.decode(), None

    return np.frombuffer(raw_data, dtype=element_type.to_numpy_type()), None


def _capnp_value_to_python_value(
    capnp_value: session_protocol_capnp.Value,
) -> tuple[Value, ExtraHeader | None]:
    """Convert a capnp value to a python value.

    Args:
        capnp_value: The value to convert.

    Returns:
        The converted value.

    Raises:
        ValueError: If the capnp value has an unknown type.
    """
    capnp_type = capnp_value.which()
    if capnp_type == "int64":
        return capnp_value.int64, None
    if capnp_type == "double":
        return capnp_value.double, None
    if capnp_type == "complex":
        return complex(capnp_value.complex.real, capnp_value.complex.imag), None
    if capnp_type == "string":
        return capnp_value.string, None
    if capnp_type == "vectorData":
        return _capnp_vector_to_value(capnp_value.vectorData)
    if capnp_type == "cntSample":
        return CntSample.from_capnp(capnp_value.cntSample), None
    if capnp_type == "triggerSample":
        return TriggerSample.from_capnp(capnp_value.triggerSample), None
    if capnp_type == "none":
        return None, None
    msg = f"Unknown capnp type: {capnp_type}"
    raise ValueError(msg)


class _VectorElementType(IntEnum):
    """Type of the elements in a vector supported by the capnp interface.

    Since the vector data is transmitted as a byte array the type of the
    elements in the vector must be specified. This enum contains all supported
    types by the capnp interface.
    """

    UINT8 = 0
    UINT16 = 1
    UINT32 = 2
    UINT64 = 3
    FLOAT = 4
    DOUBLE = 5
    STRING = 6
    COMPLEX_FLOAT = 7
    COMPLEX_DOUBLE = 8

    @classmethod
    def from_numpy_type(
        cls,
        numpy_type: np.dtype,
    ) -> _VectorElementType:
        """Construct a VectorElementType from a numpy type.

        Args:
            numpy_type: The numpy type to be converted.

        Returns:
            The VectorElementType corresponding to the numpy type.

        Raises:
            ValueError: If the numpy type has no corresponding
                VectorElementType.
        """
        if np.issubdtype(numpy_type, np.uint8):
            return cls.UINT8
        if np.issubdtype(numpy_type, np.uint16):
            return cls.UINT16
        if np.issubdtype(numpy_type, np.uint32):
            return cls.UINT32
        if np.issubdtype(numpy_type, np.uint64):
            return cls.UINT64
        if np.issubdtype(numpy_type, np.single):
            return cls.FLOAT
        if np.issubdtype(numpy_type, np.double):
            return cls.DOUBLE
        if np.issubdtype(numpy_type, np.csingle):
            return cls.COMPLEX_FLOAT
        if np.issubdtype(numpy_type, np.cdouble):
            return cls.COMPLEX_DOUBLE
        msg = f"Invalid vector element type: {numpy_type}."
        raise ValueError(msg)

    def to_numpy_type(self) -> np.dtype:
        """Convert to numpy type.

        This should always work since all relevant types are supported by
        numpy.

        Returns:
            The numpy type corresponding to the VectorElementType.
        """
        return _CAPNP_TO_NUMPY_TYPE[self]  # type: ignore[return-value]


# Static Mapping from VectorElementType to numpy type.
_CAPNP_TO_NUMPY_TYPE = {
    _VectorElementType.UINT8: np.uint8,
    _VectorElementType.UINT16: np.uint16,
    _VectorElementType.UINT32: np.uint32,
    _VectorElementType.UINT64: np.uint64,
    _VectorElementType.FLOAT: np.single,
    _VectorElementType.DOUBLE: np.double,
    _VectorElementType.STRING: str,
    _VectorElementType.COMPLEX_FLOAT: np.csingle,
    _VectorElementType.COMPLEX_DOUBLE: np.cdouble,
}


def _value_from_python_types(
    value: Any,  # noqa: ANN401
) -> capnp.lib.capnp._DynamicStructBuilder:  # noqa: SLF001
    """Create `session_protocol_capnp.Value` builder from Python types.

    Args:
        value: The value to be converted.

    Returns:
        A new message builder for `labone.core.resources.session_protocol_capnp:Value`.

    Raises:
        LabOneCoreError: If the data type of the value to be set is not supported.
    """
    request_value = session_protocol_capnp.Value.new_message()
    if isinstance(value, bool):
        request_value.int64 = int(value)
    elif np.issubdtype(type(value), np.integer):
        request_value.int64 = value
    elif np.issubdtype(type(value), np.floating):
        request_value.double = value
    elif isinstance(value, complex):
        request_value.complex = session_protocol_capnp.Complex(
            real=value.real,
            imag=value.imag,
        )
    elif isinstance(value, str):
        request_value.string = value
    elif isinstance(value, bytes):
        request_value.vectorData = session_protocol_capnp.VectorData(
            valueType=VectorValueType.BYTE_ARRAY.value,
            extraHeaderInfo=0,
            vectorElementType=_VectorElementType.UINT8.value,
            data=value,
        )
    elif isinstance(value, np.ndarray):
        vector_data = session_protocol_capnp.VectorData(
            valueType=VectorValueType.VECTOR_DATA.value,
            extraHeaderInfo=0,
            vectorElementType=_VectorElementType.from_numpy_type(value.dtype).value,
            data=value.tobytes(),
        )
        request_value.vectorData = vector_data
    else:
        msg = f"The provided value has an invalid type: {type(value)}"
        raise ValueError(
            msg,
        )
    return request_value
