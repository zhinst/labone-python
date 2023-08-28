"""T O D O result handling."""
from dataclasses import dataclass
from typing import Any, Union

import capnp

from labone.core import errors
from labone.core.resources import (  # type: ignore[attr-defined]
    result_capnp,
    session_protocol_capnp,
)


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
    def from_capnp(raw: session_protocol_capnp.TriggerSample) -> "TriggerSample":
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
    def from_capnp(raw: session_protocol_capnp.CntSample) -> "CntSample":
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


@dataclass
class AnnotatedValue:
    """Class for storing the result of a request for a node's value.

    Args:
        value: The value stored in the node.
        timestamp: Timestamp for when the value of the node was sent by
            the device to the data server.
        path: Path of the requested node.
        extra_header: For some types of vector nodes, additional information
            regarding the data. None otherwise.
    """

    value: Value
    timestamp: int
    path: str
    extra_header: Any = None

    @staticmethod
    def from_capnp(raw: session_protocol_capnp.AnnotatedValue) -> "AnnotatedValue":
        """Convert a capnp AnnotatedValue to a python AnnotatedValue."""
        return AnnotatedValue(
            value=_capnp_value_to_python_value(raw.value),
            timestamp=raw.metadata.timestamp,
            path=raw.metadata.path,
        )


def unwrap(result: result_capnp.Result) -> result_capnp.Result.ok:
    """Unwrap a result."""
    try:
        return result.ok
    except capnp.KjException:
        pass
    try:
        raise errors.LabOneCoreError(result.err.msg)
    except capnp.KjException as e:
        msg = "Whoops"
        raise RuntimeError(msg) from e
