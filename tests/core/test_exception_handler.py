from functools import partial
from pathlib import Path

import capnp
import pytest
from labone.core import _exception_handler, errors

capnp.remove_import_hook()
_testfile = Path(__file__).parent / "resources/testfile.capnp"
testfile = capnp.load(str(_testfile.resolve()))


class TestDynamicSchemaErrorHandler:
    def test_success(self):
        testfile.TestObject(name="text")
        _exception_handler._capnp_dynamic_schema_error_handler(
            testfile.TestObject,
            name="text",
        )

    def test_error_cases(self):
        with pytest.raises(capnp.KjException):
            testfile.TestObject(foo=3)
        with pytest.raises(errors.LabOneCoreError):
            _exception_handler._capnp_dynamic_schema_error_handler(
                testfile.TestObject,
                foo=3,
            )


class TestWrapDynamicCapnp:
    @staticmethod
    def dynamic_test_function(err, raise_=True, rval=None):  # noqa: FBT002
        """A mock of `capnp.lib.capnp._DynamicCapabilityClient`"""

        class RemoteTestPromise:
            """A mock of `capnp.lib.capnp._RemotePromise`."""

            def __init__(self, err, raise_, rval) -> None:
                self._err = err
                self._raise = raise_
                self._rval = rval

            async def a_wait(self):
                if self._raise:
                    raise self._err
                return self._rval

        return RemoteTestPromise(err, raise_, rval)

    @pytest.mark.parametrize(
        ("awaitable_error", "expected"),
        [
            (capnp.KjException("text"), errors.LabOneConnectionError("text")),
            (RuntimeError("text"), RuntimeError("text")),
            (ValueError("text"), ValueError("text")),
        ],
    )
    @pytest.mark.asyncio()
    async def test_error_cases(self, awaitable_error, expected):
        f = partial(TestWrapDynamicCapnp.dynamic_test_function, awaitable_error)
        with pytest.raises(type(awaitable_error)):
            await f().a_wait()
        with pytest.raises(type(expected)):  # noqa: PT012
            func = _exception_handler.wrap_dynamic_capnp(f)
            await func()

    @pytest.mark.asyncio()
    async def test_success(self):
        rval = 123
        f = partial(
            TestWrapDynamicCapnp.dynamic_test_function,
            None,
            raise_=False,
            rval=rval,
        )
        assert await f().a_wait() == rval
