"""Module to handle the creation and conversion of values between capnp and python.

The relevant class is the dataclass `AnnotatedValue`. It is used as the main
data container for all values send to and received by the kernel/server.
It has both a function to convert a capnp message to a python object and vice
versa.
"""

from __future__ import annotations

import logging
import typing as t
from dataclasses import dataclass

import numpy as np
from typing_extensions import TypeAlias

from labone.core.errors import (
    LabOneCoreError,
    SHFHeaderVersionNotSupportedError,
    get_streaming_error,
)
from labone.core.helper import (
    LabOneNodePath,
    VectorElementType,
    VectorValueType,
)
from labone.core.shf_vector_data import (
    ExtraHeader,
    SHFDemodSample,
    encode_shf_vector_data_struct,
    parse_shf_vector_data_struct,
    preprocess_complex_shf_waveform_vector,
)

if t.TYPE_CHECKING:  # pragma: no cover
    import zhinst.comms

logger = logging.getLogger(__name__)


CapnpInput: TypeAlias = dict[str, t.Any]


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
        timestamp: Timestamp at which the device sent the value.
            (Only relevant for received values.)
        extra_header: For some types of vector nodes, additional information
            regarding the data. None otherwise.
    """

    value: Value
    path: LabOneNodePath
    timestamp: int | None = None
    extra_header: ExtraHeader | None = None

    def __repr__(self) -> str:
        if self.extra_header is None:  # pragma: no cover
            return (
                f"AnnotatedValue(value={self.value}, path={self.path}, "
                f"timestamp={self.timestamp})"
            )
        return (  # pragma: no cover
            f"AnnotatedValue(value={self.value}, path={self.path}, "
            f"timestamp={self.timestamp}, extra_header={self.extra_header})"
        )

    @staticmethod
    def from_capnp(raw: zhinst.comms.DynamicStruct) -> AnnotatedValue:
        """Convert a capnp AnnotatedValue to a python AnnotatedValue.

        Args:
            raw: The capnp AnnotatedValue to convert

        Returns:
            The converted AnnotatedValue.

        Raises:
            ValueError: If the capnp value has an unknown type or can not be parsed.
        """
        try:
            try:
                value, extra_header = _capnp_value_to_python_value(raw.value)
            except AttributeError:
                value, extra_header = None, None
            try:
                timestamp = raw.metadata.timestamp
            except AttributeError:
                timestamp = None
            try:
                path = raw.metadata.path
            except AttributeError:
                path = ""
            return AnnotatedValue(
                value=value,
                timestamp=timestamp,
                path=path,
                extra_header=extra_header,
            )
        except RuntimeError as e:
            raise LabOneCoreError(str(e)) from e


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
    def from_capnp(raw: zhinst.comms.DynamicStruct) -> TriggerSample:
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
    def from_capnp(raw: zhinst.comms.DynamicStruct) -> CntSample:
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
Value = t.Union[
    int,
    float,
    str,
    complex,
    np.ndarray,
    SHFDemodSample,
    TriggerSample,
    CntSample,
    list[TriggerSample],
    list[CntSample],
    None,
]


def _capnp_vector_to_value(
    vector_data: zhinst.comms.DynamicStruct,
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

    Raises:
        ValueError: If the vector data can not be parsed.
    """
    raw_data = vector_data.data
    element_type = VectorElementType(vector_data.vectorElementType)
    generic_vector_types = [VectorValueType.VECTOR_DATA, VectorValueType.BYTE_ARRAY]
    if vector_data.valueType not in generic_vector_types:
        # For the time being we need to manually untangle the shf vector types.
        # since it is planed to do this directly on the server side this logic
        # is outsourced in a different module.
        try:
            return parse_shf_vector_data_struct(vector_data)
        except ValueError:  # pragma: no cover
            logger.error(  # noqa: TRY400
                "received unknown shf vector type. Please update the API to the "
                "latest version.",
            )
            raise
        except SHFHeaderVersionNotSupportedError as e:  # pragma: no cover
            # The version of the shf vector data is not supported by the client.
            logger.error(  # noqa: TRY400
                "%s Please update the API to the latest version.",
                e.args[0],
            )
            raise ValueError(e.args[0]) from e

    if element_type == VectorElementType.STRING:
        # Special case for strings which are send as byte arrays
        return raw_data.decode(), None

    return np.frombuffer(raw_data, dtype=element_type.to_numpy_type()), None


def _capnp_value_to_python_value(
    capnp_value: zhinst.comms.DynamicStruct,
) -> tuple[Value, ExtraHeader | None]:
    """Convert a capnp value to a python value.

    Args:
        capnp_value: The value to convert.

    Returns:
        The converted value.

    Raises:
        ValueError: If the capnp value has an unknown type or can not be parsed.
        LabOneCoreError: If the capnp value is a streaming error.
    """
    if hasattr(capnp_value, "int64"):
        return capnp_value.int64, None
    if hasattr(capnp_value, "double"):
        return capnp_value.double, None
    if hasattr(capnp_value, "complex"):
        return complex(capnp_value.complex.real, capnp_value.complex.imag), None
    if hasattr(capnp_value, "string"):
        return capnp_value.string, None
    if hasattr(capnp_value, "vectorData"):
        return _capnp_vector_to_value(capnp_value.vectorData)
    if hasattr(capnp_value, "cntSample"):
        return CntSample.from_capnp(capnp_value.cntSample), None
    if hasattr(capnp_value, "vectorCntSamples"):
        return [
            CntSample.from_capnp(sample) for sample in capnp_value.vectorCntSamples
        ], None
    if hasattr(capnp_value, "triggerSample"):
        return TriggerSample.from_capnp(capnp_value.triggerSample), None
    if hasattr(capnp_value, "vectorTriggerSamples"):
        return [
            TriggerSample.from_capnp(sample)
            for sample in capnp_value.vectorTriggerSamples
        ], None
    if hasattr(capnp_value, "none"):
        return None, None
    if hasattr(capnp_value, "streamingError"):
        raise get_streaming_error(capnp_value.streamingError)
    msg = f"Unknown capnp type: {dir(capnp_value)}"
    raise ValueError(msg)


def _numpy_vector_to_capnp_vector(
    np_vector: np.ndarray,
    *,
    path: LabOneNodePath,
) -> CapnpInput:
    """Convert a numpy vector to a capnp vector.

    Args:
        np_vector: The numpy vector to convert.
        path: The path of the node the vector belongs to.
        reflection: The reflection server used for the conversion.

    Returns:
        The converted capnp vector.

    LabOneCoreError: If the numpy type has no corresponding
        VectorElementType.
    """
    request_value: dict[str, t.Any] = {}
    request_value["extraHeaderInfo"] = 0
    request_value["valueType"] = VectorValueType.VECTOR_DATA.value
    np_data = np_vector
    np_vector_type = np_vector.dtype
    if np.iscomplexobj(np_vector) and "waveforms" in path.lower():
        np_data, np_vector_type = preprocess_complex_shf_waveform_vector(np_vector)
    request_value["data"] = np_data.tobytes()
    try:
        request_value["vectorElementType"] = VectorElementType.from_numpy_type(
            np_vector_type,
        ).value
    except ValueError as e:
        msg = f"Unsupported numpy type: {np_vector_type}"
        raise ValueError(msg) from e
    return request_value


def value_from_python_types(
    value: t.Any,  # noqa: ANN401
    *,
    path: LabOneNodePath,
) -> CapnpInput:
    """Create `Value` builder from Python types.

    Args:
        value: The value to be converted.
        path: The path of the node the value belongs to.
        reflection: The reflection server used for the conversion.

    Returns:
        A new message builder for `capnp:Value`.

    Raises:
        LabOneCoreError: If the data type of the value to be set is not supported.
    """
    request_value: dict[str, t.Any] = {}
    if isinstance(value, bool):
        request_value["int64"] = int(value)
    elif np.issubdtype(type(value), np.integer):
        request_value["int64"] = value
    elif np.issubdtype(type(value), np.floating):
        request_value["double"] = value
    elif isinstance(value, complex):
        request_value["complex"] = {"real": value.real, "imag": value.imag}
    elif isinstance(value, str):
        request_value["string"] = value
    elif isinstance(value, bytes):
        request_value["vectorData"] = {
            "valueType": VectorValueType.BYTE_ARRAY.value,
            "extraHeaderInfo": 0,
            "vectorElementType": VectorElementType.UINT8.value,
            "data": value,
        }
    elif isinstance(value, np.ndarray):
        request_value["vectorData"] = _numpy_vector_to_capnp_vector(
            value,
            path=path,
        )
    else:
        msg = f"The provided value has an invalid type: {type(value)}"
        raise ValueError(
            msg,
        )
    return request_value


def value_from_python_types_dict(annotated_value: AnnotatedValue) -> CapnpInput:
    """Create `Value` builder from Python types.

    Note:
        This function is logically similar to `_value_from_python_types`,
        except for its extension of handling numpy arrays and shf vectors.
        However, this function does not require a reflection server as an argument.
        Instead of creating a capnp message via new_message, it does so by
        defining a dictionary as a return value. Both approaches are
        accepted by the capnp library.

    Args:
        annotated_value: The value to be converted.

    Returns:
        A new message builder for `capnp:Value`.

    Raises:
        LabOneCoreError: If the data type of the value to be set is not supported.
    """
    if (
        annotated_value.extra_header is not None
        and isinstance(
            annotated_value.value,
            (np.ndarray),
        )
        or isinstance(annotated_value.value, SHFDemodSample)
    ):
        return {
            "vectorData": encode_shf_vector_data_struct(
                data=annotated_value.value,
                extra_header=annotated_value.extra_header,  # type: ignore[union-attr, arg-type]
            ),
        }

    type_to_message = {
        bool: lambda x: {"int64": int(x)},
        np.integer: lambda x: {"int64": x},
        np.floating: lambda x: {"double": x},
        complex: lambda x: {"complex": {"real": x.real, "imag": x.imag}},
        str: lambda x: {"string": x},
        bytes: lambda x: {
            "vectorData": {
                "valueType": VectorValueType.BYTE_ARRAY.value,
                "extraHeaderInfo": 0,
                "vectorElementType": VectorElementType.UINT8.value,
                "data": x,
            },
        },
        np.ndarray: lambda x: {
            "vectorData": {
                "valueType": VectorValueType.VECTOR_DATA.value,
                "extraHeaderInfo": 0,
                "vectorElementType": VectorElementType.from_numpy_type(x.dtype).value,
                "data": x.tobytes(),
            },
        },
    }

    value = annotated_value.value
    for type_, message_builder in type_to_message.items():  # pragma: no cover
        if isinstance(value, type_) or np.issubdtype(type(value), type_):
            return message_builder(value)

    msg = (
        "The provided value has an invalid type "
        "or missing extra header: {type(value)}"
    )  # pragma: no cover
    raise ValueError(msg)  # pragma: no cover
