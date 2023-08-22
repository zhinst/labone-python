"""Exception handler module for connection errors.

This module provides a helper functionality to wrap asyncronous callables
and catch errors happening within them.
"""
from functools import wraps
from typing import Callable, TypeVar

import capnp

from labone.core import errors

T = TypeVar("T")


def _capnp_dynamic_schema_error_handler(  # noqa: D417
    callable_: Callable[..., T],
    *args,
    **kwargs,
) -> T:
    """Handler for dynamic `capnp` schema errors.

    Translates `capnp.lib.capnp.KjException` exceptions into library errors.

    Args:
        callable_: A callable.

    Raises:
        LabOneCoreError: Input values do not match the schema.
    """
    try:
        return callable_(*args, **kwargs)
    except capnp.lib.capnp.KjException as error:
        raise errors.LabOneCoreError(str(error)) from error


def wrap_dynamic_capnp(
    callable_: Callable[..., capnp.lib.capnp._RemotePromise],  # noqa: SLF001
) -> Callable[..., capnp.lib.capnp._RemotePromise]:  # noqa: SLF001
    """Wraps a dynamic `capnp` callable.

    Translates `capnp` exceptions into library errors.
    Calls the returned promise's `a_wait()` function, therefore
    the wrapped functions only need to be awaited.

    The decorator does two things:

        - Catches schema errors, for example, invalid input types

        - Catches connection errors

    Example usage::

        from labone.core._exception_handler import wrap_dynamic_capnp

        coro = wrap_dynamic_capnp(dynamic_capnp_callable)
        await coro(1, 2)

    Args:
        callable_: A callable that returns `capnp.lib.capnp._RemotePromise`.

    Raises:
        LabOneCoreError: Schema error.
        LabOneConnectionError: Connection error.
    """

    @wraps(callable_)
    async def wrapper(*args, **kwargs) -> capnp.lib.capnp._Response:  # noqa: SLF001
        try:
            coro = _capnp_dynamic_schema_error_handler(callable_, *args, **kwargs)
            return await coro.a_wait()
        # Handle connection errors
        except capnp.lib.capnp.KjException as error:
            raise errors.LabOneConnectionError(str(error)) from error

    return wrapper
