"""Type conversions."""
from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import TYPE_CHECKING, Any, Union

import numpy as np

from labone.core import errors
from labone.core.resources import session_protocol_capnp  # type: ignore[attr-defined]

if TYPE_CHECKING:
    import capnp


@dataclass
class TriggerSample:
    """TriggerSample."""

    timestamp: int
    sample_tick: int
    trigger: int
    missed_triggers: int
    awg_trigger: int
    dio: int
    sequence_index: int

    @staticmethod
    def from_capnp(raw: session_protocol_capnp.TriggerSample) -> TriggerSample:
        """Convert a capnp TriggerSample to a python TriggerSample."""
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
    """CntSample."""

    timestamp: int
    counter: int
    trigger: int

    @staticmethod
    def from_capnp(raw: session_protocol_capnp.CntSample) -> CntSample:
        """Convert a capnp CntSample to a python CntSample."""
        return CntSample(
            timestamp=raw.timestamp,
            counter=raw.counter,
            trigger=raw.trigger,
        )


Value = Union[int, float, str, complex, TriggerSample, CntSample]


def _capnp_value_to_python_value(
    capnp_value: session_protocol_capnp.Value,
) -> Value:
    """Convert a capnp value to a python value.

    Args:
        capnp_value: The value to convert.

    Returns:
        The converted value.
    """
    capnp_type = capnp_value.which()
    if capnp_type == "int64":
        return capnp_value.int64
    if capnp_type == "double":
        return capnp_value.double
    if capnp_type == "complex":
        return complex(capnp_value.complex.real, capnp_value.complex.imag)
    if capnp_type == "string":
        return capnp_value.string
    if capnp_type == "vectorData":
        return capnp_value.vectorData  # T O D O
    if capnp_type == "cntSample":
        return CntSample.from_capnp(capnp_value.cntSample)
    if capnp_type == "triggerSample":
        return TriggerSample.from_capnp(capnp_value.triggerSample)
    msg = f"Unknown capnp type: {capnp_type}"
    raise ValueError(msg)


class VectorElementType(IntEnum):
    """Vector element type.

    TODO: More precice docstring.
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
        cls: VectorElementType,
        value,
    ) -> VectorElementType:
        """Create from numpy type."""
        if np.issubdtype(value, np.uint8):
            return cls.UINT8
        if np.issubdtype(value, np.uint16):
            return cls.UINT16
        if np.issubdtype(value, np.uint32):
            return cls.UINT32
        if np.issubdtype(value, np.uint64):
            return cls.UINT64
        if np.issubdtype(value, np.single):
            return cls.FLOAT
        if np.issubdtype(value, np.double):
            return cls.DOUBLE
        if np.issubdtype(value, np.csingle):
            return cls.COMPLEX_FLOAT
        if np.issubdtype(value, np.cdouble):
            return cls.COMPLEX_DOUBLE
        msg = f"Invalid vector element type: {value}."
        raise ValueError(msg)

    def to_numpy_type(self):
        """Convert to numpy type."""
        zi_to_numpy_type = {
            VectorElementType.UINT8: np.uint8,
            VectorElementType.UINT16: np.uint16,
            VectorElementType.UINT32: np.uint32,
            VectorElementType.UINT64: np.uint64,
            VectorElementType.FLOAT: np.single,
            VectorElementType.DOUBLE: np.double,
            VectorElementType.STRING: str,
            VectorElementType.COMPLEX_FLOAT: np.csingle,
            VectorElementType.COMPLEX_DOUBLE: np.cdouble,
        }
        return zi_to_numpy_type[self]


class VectorValueType(IntEnum):
    """Vector value type.

    TODO: More precide docstring
    """
    BYTE_ARRAY = 7
    VECTOR_DATA = 67
    SHF_GENERATOR_WAVEFORM_VECTOR_DATA = 69
    SHF_RESULT_LOGGER_VECTOR_DATA = 70
    SHF_SCOPE_VECTOR_DATA = 71
    SHF_DEMODULATOR_VECTOR_DATA = 72


@dataclass
class AnnotatedValue:
    """Class for storing the result of a request for a node's value.

    Args:
        value: The value stored in the node.
        path: Path of the requested node.
        timestamp: Timestamp for when the value of the node was sent by
            the device to the data server.
        extra_header: For some types of vector nodes, additional information
            regarding the data. None otherwise.
    """

    value: Value
    path: str
    # T O D O: Send wont have `timestamp` field, make sure capnp schema matches
    timestamp: int | None = None
    extra_header: Any = None

    @staticmethod
    def from_capnp(raw: session_protocol_capnp.AnnotatedValue) -> AnnotatedValue:
        """Convert a capnp AnnotatedValue to a python AnnotatedValue."""
        return AnnotatedValue(
            value=_capnp_value_to_python_value(raw.value),
            timestamp=raw.metadata.timestamp,
            path=raw.metadata.path,
        )

    @staticmethod
    def from_python_types(
        value: Any,
        path: str,
        extra_header: Any = None,
    ) -> AnnotatedValue:
        """Convert a python AnnotatedValue to a capnp AnnotatedValue."""
        message = session_protocol_capnp.AnnotatedValue.new_message()
        try:
            message.metadata.path = path
        except Exception:  # noqa: BLE001
            msg = "`path` must be a string."
            raise TypeError(msg)  # noqa: TRY200, B904
        request_value = _value_from_python_types(value)
        return AnnotatedValue(
            value=request_value,
            path=path,
            extra_header=extra_header,
        )


def _value_from_python_types(
    value: Any
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
            real=value.real, imag=value.imag,
        )
    elif isinstance(value, str):
        request_value.string = value
    elif isinstance(value, bytes):
        request_value.vectorData = session_protocol_capnp.VectorData(
            valueType=VectorValueType.BYTE_ARRAY.value,
            extraHeaderInfo=0,
            vectorElementType=VectorElementType.UINT8.value,
            data=value,
        )
    elif isinstance(value, np.ndarray):
        vector_data = session_protocol_capnp.VectorData(
            valueType=VectorValueType.VECTOR_DATA.value,
            extraHeaderInfo=0,
            vectorElementType=VectorElementType.from_numpy_type(value.dtype).value,
            data=value.tobytes(),
        )
        request_value.vectorData = vector_data
    else:
        msg = f"The provided value has an invalid type: {type(value)}"
        raise errors.LabOneCoreError(
            msg,
        )
    return request_value
