"""Helper module for the sync wrapper."""
import asyncio
import threading
import typing as t
from functools import wraps

from unsync import unsync, Unfuture

T = t.TypeVar("T")

FALLBACK_TIMEOUT = 10.0


def make_sync(func: t.Callable[..., t.Awaitable[T]]) -> t.Callable[..., T]:
    """Synchronize an async function."""

    @wraps(func)
    def wrapper(*args, **kwargs) -> T:
        if threading.current_thread() != unsync.thread:
            timeout = kwargs.get("timeout", FALLBACK_TIMEOUT)
            try:
                return unsync(asyncio.wait_for)(
                    func(*args, **kwargs),
                    timeout=timeout,
                ).result()
            except asyncio.TimeoutError as e:
                msg = (
                    f"Function {func.__code__.co_qualname} timed out"
                    f"after {timeout} seconds",
                )
                raise TimeoutError(msg) from e
        return func(*args, **kwargs)

    return wrapper


def make_semi_sync(
    func: t.Callable[..., t.Awaitable[T]]
) -> t.Callable[..., Unfuture[T]]:
    """Synchronize an async function."""

    @wraps(func)
    def wrapper(*args, **kwargs) -> Unfuture[T]:
        if threading.current_thread() != unsync.thread:
            timeout = kwargs.get("timeout", FALLBACK_TIMEOUT)
            try:
                return unsync(asyncio.wait_for)(
                    func(*args, **kwargs),
                    timeout=timeout,
                )
            except asyncio.TimeoutError as e:
                msg = (
                    f"Function {func.__code__.co_qualname} timed out"
                    f"after {timeout} seconds",
                )
                raise TimeoutError(msg) from e
        return func(*args, **kwargs)

    return wrapper