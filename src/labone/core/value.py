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
from packaging import version
from typing_extensions import TypeAlias

from labone.core import hpk_schema
from labone.core.errors import LabOneCoreError, raise_streaming_error
from labone.core.helper import (
    LabOneNodePath,
    VectorElementType,
    VectorValueType,
)
from labone.core.shf_vector_data import (
    ShfDemodulatorVectorData,
    ShfGeneratorWaveformVectorData,
    ShfPidVectorData,
    ShfResultLoggerVectorData,
    ShfScopeVectorData,
    parse_shf_vector_from_vector_data,
    preprocess_complex_shf_waveform_vector,
    supports_shf_vector_parsing_from_vector_data,
)

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
    """

    value: Value
    path: LabOneNodePath
    timestamp: int | None = None

    def __repr__(self) -> str:
        return (
            f"AnnotatedValue(value={self.value}, path={self.path}, "
            f"timestamp={self.timestamp})"
        )

    @staticmethod
    def from_capnp(raw: hpk_schema.AnnotatedValue) -> AnnotatedValue:
        """Convert a capnp AnnotatedValue to a python AnnotatedValue.

        Args:
            raw: The capnp AnnotatedValue to convert

        Returns:
            The converted AnnotatedValue.
        """
        try:
            value = _capnp_value_to_python_value(raw.value)
        except AttributeError:
            value = None
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
        )


# All possible types of values that can be stored in a node.
Value = t.Union[
    bool,
    int,
    float,
    str,
    complex,
    np.ndarray,
    ShfDemodulatorVectorData,
    ShfResultLoggerVectorData,
    ShfScopeVectorData,
    ShfPidVectorData,
    ShfGeneratorWaveformVectorData,
    hpk_schema.CntSample,
    hpk_schema.TriggerSample,
    list[hpk_schema.CntSample],
    list[hpk_schema.TriggerSample],
    dict,
    None,
    hpk_schema.VectorData,
]


def _parse_vector_data(
    value: hpk_schema.VectorData,
) -> (
    ShfScopeVectorData
    | ShfResultLoggerVectorData
    | ShfGeneratorWaveformVectorData
    | np.ndarray
    | str
):
    """Parse a capnp vector data message.

    Args:
        value: The capnp vector data message to parse.

    Returns:
        The parsed vector data.
    """
    element_type = VectorElementType(value.vectorElementType)
    if element_type == VectorElementType.STRING:
        # Special case for strings which are send as byte arrays
        return value.data.decode()
    if supports_shf_vector_parsing_from_vector_data(value.valueType):
        return parse_shf_vector_from_vector_data(value)
    return np.frombuffer(value.data, dtype=element_type.to_numpy_type())


_TO_PYTHON_PARSER = {
    "none": lambda _: None,
    "vectorData": _parse_vector_data,
    "largeVectorData": lambda value: np.frombuffer(
        value.dataSegments,
        dtype=VectorElementType(value.vectorElementType).to_numpy_type(),
    ),
    "streamingError": lambda value: raise_streaming_error(
        t.cast(hpk_schema.Error, value),
    ),
    "shfDemodData": lambda value: ShfDemodulatorVectorData(
        x=np.array(value.x, copy=False),
        y=np.array(value.y, copy=False),
        properties=value.properties,
    ),
    "shfResultLoggerData": lambda value: ShfResultLoggerVectorData(
        vector=(
            np.array(value.vector.real, copy=False)
            if hasattr(value.vector, "real")
            else np.array(value.vector.complex, copy=False)
        ),
        properties=value.properties,
    ),
    "shfScopeData": lambda value: ShfScopeVectorData(
        vector=(
            np.array(value.vector.real, copy=False)
            if hasattr(value.vector, "real")
            else np.array(value.vector.complex, copy=False)
        ),
        properties=value.properties,
    ),
    "shfGeneratorWaveformData": lambda value: ShfGeneratorWaveformVectorData(
        complex=np.array(value.complex, copy=False),
    ),
    "shfPidData": lambda value: ShfPidVectorData(
        value=np.array(value.value, copy=False),
        error=np.array(value.error, copy=False),
        properties=value.properties,
    ),
}


def _capnp_value_to_python_value(
    capnp_value: hpk_schema.Value,
) -> Value:
    """Convert a capnp value to a python value.

    Args:
        capnp_value: The value to convert.

    Returns:
        The converted value.

    Raises:
        ValueError: If the capnp value can not be parsed.
        LabOneCoreError: If the capnp value is a streaming error.
    """
    union_element_name = capnp_value.get_union_element_name()
    return _TO_PYTHON_PARSER.get(union_element_name, lambda x: x)(
        getattr(capnp_value, union_element_name),
    )


def _numpy_vector_to_capnp_vector(
    np_vector: np.ndarray,
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
    request_value["data"] = np_data.tobytes()
    try:
        request_value["vectorElementType"] = VectorElementType.from_numpy_type(
            np_vector_type,
        ).value
    except ValueError as e:
        msg = f"Unsupported numpy type: {np_vector_type}"
        raise ValueError(msg) from e
    return {"vectorData": request_value}


_FROM_PYTHON_PARSER = {
    dict: lambda x, _: x,
    bool: lambda x, compat_version: (
        {"bool": x}
        if compat_version >= version.Version("1.13.0")
        else {"int64": int(x)}
    ),
    int: lambda x, _: {"int64": x},
    float: lambda x, _: {"double": x},
    complex: lambda x, _: {"complex": x},
    str: lambda x, _: {"string": x},
    bytes: lambda x, _: {
        "vectorData": {
            "valueType": VectorValueType.BYTE_ARRAY.value,
            "extraHeaderInfo": 0,
            "vectorElementType": VectorElementType.UINT8.value,
            "data": x,
        },
    },
    np.ndarray: lambda x, _: _numpy_vector_to_capnp_vector(x),
    ShfDemodulatorVectorData: lambda x, _: {
        "shfDemodData": {"properties": x.properties, "x": x.x, "y": x.y},
    },
    ShfResultLoggerVectorData: lambda x, _: {
        "shfResultLoggerData": {
            "properties": x.properties,
            "vector": {"real" if x.vector.dtype.kind == "f" else "complex": x.vector},
        },
    },
    ShfScopeVectorData: lambda x, _: {
        "shfScopeData": {
            "properties": x.properties,
            "vector": {"real" if x.vector.dtype.kind == "f" else "complex": x.vector},
        },
    },
    ShfGeneratorWaveformVectorData: lambda x, compat_version: (
        {"shfGeneratorWaveformData": {"complex": x.complex}}
        if compat_version >= version.Version("1.15.0")
        else preprocess_complex_shf_waveform_vector(x.complex)
    ),
    ShfPidVectorData: lambda x, _: {
        "shfPidData": {
            "properties": x.properties,
            "value": x.value,
            "error": x.error,
        },
    },
    hpk_schema.CntSample: lambda x, _: {"cntSample": x},
    hpk_schema.TriggerSample: lambda x, _: {"triggerSample": x},
    list[hpk_schema.CntSample]: lambda x, _: {"cntSample": x},
    list[hpk_schema.TriggerSample]: lambda x, _: {"triggerSample": x},
}


def value_from_python_types(
    value: Value,
    *,
    capability_version: version.Version,
) -> CapnpInput:
    """Create `Value` builder from Python types.

    Args:
        value: The value to be converted.
        capability_version: The version of the capability to be used.

    Returns:
        A new message builder for `capnp:Value`.

    Raises:
        LabOneCoreError: If the data type of the value to be set is not supported.
    """
    try:
        return _FROM_PYTHON_PARSER[type(value)](value, capability_version)
    except KeyError as e:
        msg = f"Unsupported data type: {type(value)}"
        raise LabOneCoreError(msg) from e
