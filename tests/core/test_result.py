"""Tests unwrapping of a capnp result."""

from dataclasses import dataclass

import capnp
import pytest
from labone.core import errors
from labone.core.result import unwrap


@dataclass
class FakeError:
    kind: int
    message: str
    code: int = 0
    category: str = ""


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
    ("error_kind", "exception"),
    [
        (1, errors.CancelledError),
        (3, errors.NotFoundError),
        (4, errors.OverwhelmedError),
        (5, errors.BadRequestError),
        (6, errors.UnimplementedError),
        (7, errors.InternalError),
        (8, errors.UnavailableError),
        (9, errors.LabOneTimeoutError),
    ],
)
def test_unwrap_error_generic(error_kind, exception):
    msg = FakeResult()
    msg.err = FakeError(kind=error_kind, message="test")
    with pytest.raises(exception, match="test"):
        unwrap(msg)


def test_invalid_capnp_response():
    msg = FakeResult()
    with pytest.raises(errors.LabOneCoreError):
        unwrap(msg)
