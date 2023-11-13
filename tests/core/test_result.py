"""Tests unwrapping of a capnp result."""
from dataclasses import dataclass

import capnp
import pytest
from labone.core import errors
from labone.core.result import unwrap


@dataclass
class FakeError:
    code: int
    message: str


class FakeResult:
    def __init__(self):
        self._ok = None
        self._err = None

    @property
    def ok(self):
        if self._err is not None or self._ok is None:
            msg = "test"
            raise capnp.KjException(msg)
        return self._ok

    @ok.setter
    def ok(self, value):
        self._ok = value

    @property
    def err(self):
        if self._err is None:
            msg = "test"
            raise capnp.KjException(msg)
        return self._err

    @err.setter
    def err(self, value):
        self._err = value


def test_unwrap_ok():
    msg = FakeResult()
    msg.ok = "test"
    assert unwrap(msg) == "test"


@pytest.mark.parametrize(
    ("error_code", "exception"),
    [
        (0x8000, errors.LabOneCoreError),
        (0x800C, errors.LabOneConnectionError),
        (0x800D, errors.LabOneTimeoutError),
        (0x8013, errors.LabOneReadOnlyError),
        (0x8014, errors.KernelNotFoundError),
        (0x8015, errors.DeviceInUseError),
        (0x8016, errors.InterfaceMismatchError),
        (0x8017, errors.LabOneTimeoutError),
        (0x8018, errors.DifferentInterfaceInUseError),
        (0x8019, errors.FirmwareUpdateRequiredError),
        (0x801B, errors.DeviceNotFoundError),
        (0x8020, errors.LabOneWriteOnlyError),
    ],
)
def test_unwrap_error_generic(error_code, exception):
    msg = FakeResult()
    msg.err = FakeError(code=error_code, message="test")
    with pytest.raises(exception, match="test"):
        unwrap(msg)


def test_invalid_capnp_response():
    msg = FakeResult()
    with pytest.raises(errors.LabOneCoreError):
        unwrap(msg)
