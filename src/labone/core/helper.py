"""Module for stuff that is shared between different modules.

This module bypasses the circular dependency between the modules
within the core.
"""
from enum import IntEnum

import capnp
import numpy as np


def request_field_type_description(
    request: capnp.lib.capnp._Request,  # noqa: SLF001
    field: str,
) -> str:
    """Get given `capnp` request field type description.

    Args:
        request: Capnp request.
        field: Field name of the request.
    """
    return request.schema.fields[field].proto.slot.type.which()


class VectorValueType(IntEnum):
    """Mapping of the vector value type.

    VectorValueType specifies the type of the vector. It uses (a subset) of
    values from `ZIValueType_enum` from the C++ client. The most commonly used
    types are "VECTOR_DATA" and "BYTE_ARRAY". Some vectors use a different
    format, e.g. for SHF devices.
    """

    BYTE_ARRAY = 7
    VECTOR_DATA = 67
    SHF_GENERATOR_WAVEFORM_VECTOR_DATA = 69
    SHF_RESULT_LOGGER_VECTOR_DATA = 70
    SHF_SCOPE_VECTOR_DATA = 71
    SHF_DEMODULATOR_VECTOR_DATA = 72


class VectorElementType(IntEnum):
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
    ) -> "VectorElementType":
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
