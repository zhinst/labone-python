"""Module for stuff that is shared between different modules.

This module bypasses the circular dependency between the modules
within the core.
"""

from __future__ import annotations

import asyncio
import logging
import typing as t
import weakref
from contextlib import asynccontextmanager
from enum import IntEnum

import asyncio_atexit  # type: ignore [import]
import capnp
import numpy as np
from typing_extensions import TypeAlias

from labone.core.errors import LabOneCoreError, UnavailableError

logger = logging.getLogger(__name__)

LabOneNodePath: TypeAlias = str
CapnpCapability: TypeAlias = capnp.lib.capnp._DynamicCapabilityClient  # noqa: SLF001
CapnpStructReader: TypeAlias = capnp.lib.capnp._DynamicStructReader  # noqa: SLF001
CapnpStructBuilder: TypeAlias = capnp.lib.capnp._DynamicStructBuilder  # noqa: SLF001


class CapnpLock:
    """Lock that can be used to enforce the existence of the kj event loop.

    This lock MUST be created from a LoopManager and not as a standalone.
    If not it does not offer any guarantees.

    The usage is fairly simple. Whenever the `lock` context manager is entered
    it is guaranteed that the LoopManager will not destroy the kj event loop.
    The context manager can not be entered once the kj event loop is destroyed.
    Note that once the kj event loop that was running when this lock was created
    is destroyed there is no way to reactivate a lock.

    Args:
        lock: Asyncio lock that is used to manage the lifetime.
    """

    def __init__(self, lock: asyncio.Lock):
        self._lock: asyncio.Lock | None = lock

    async def destroy(self) -> None:
        """Destroy a lock and prevent it form ever being locked again.

        Note that this function waits until the lock is released if its
        locked.
        """
        if self._lock is None:
            return
        await self._lock.acquire()
        self._lock = None

    @asynccontextmanager
    async def lock(self) -> t.AsyncGenerator[None, None]:
        """Locks the lock.

        This ensures that the kj event loop that was running when this
        lock was created stays alive.
        """
        if self._lock is None:
            msg = (
                "The event loop in which this object was created is closed. "
                "No further operations are possible."
            )
            raise UnavailableError(msg)
        async with self._lock:
            yield


class LoopManager:
    """Capnp Loop Manager.

    This class manages the lifetime of the kj event loop.
    Pycapnp attaches its own event loop called kj event loop
    to the asyncio event loop. The tricky part is how long this
    event loop should stay alive. Since the lifetime is not always
    clear this loop manager is manages the lifetime. Once the destroy
    method is called the kj event loop is exited.

    Note that all pycapnp objects, especially the rpc related objects are
    no longer valid once the kj even loop is destroyed. To prevent destruction
    of the kj event loop while using these object the `create_lock` method can
    be used to create locks which prevent the destruction.

    Since the reation of the kj event loop is and async operation this class
    needs to be created using the `create` method.

    Args:
        loop: The kj event loop that should be managed.
    """

    def __init__(self, loop: capnp.kj_loop):
        self._locks: weakref.WeakSet[CapnpLock] = weakref.WeakSet()
        self._loop = loop
        self._active = True

    @staticmethod
    async def exists() -> bool:
        """Check if a kj event loop is already running.

        Returns:
            True if a kj event loop is already running, False otherwise.
        """
        return hasattr(asyncio.get_running_loop(), "_zi_loop_manager")

    @staticmethod
    def get_running() -> LoopManager:
        """Get the running loop manager.

        There must only be one loop manager running at a time. This method
        returns the running loop manager.

        Returns:
            The running loop manager.

        Raises:
            CoreError: If no loop manager is running.
        """
        try:
            return getattr(  # noqa: B009
                asyncio.get_running_loop(),
                "_zi_loop_manager",
            )
        except AttributeError:  # pragma: no cover
            msg = "No loop manager is running."
            raise LabOneCoreError(msg) from None

    @staticmethod
    async def create() -> LoopManager:
        """Create a new loop manager.

        Returns:
            The newly created loop manager.

        Raises:
            CoreError: If more than one kj event loop is running.
        """
        if (
            hasattr(asyncio.get_running_loop(), "_kj_loop")
            or await LoopManager.exists()
        ):
            msg = "More than one loop manager is not supported."
            raise LabOneCoreError(msg)

        loop = capnp.kj_loop()
        logger.debug("kj event loop attached to asyncio event loop %s", id(loop))
        await loop.__aenter__()
        manager = LoopManager(loop)
        setattr(asyncio.get_running_loop(), "_zi_loop_manager", manager)  # noqa: B010
        return manager

    async def destroy(self) -> None:
        """Destroy the managed kj event loop.

        Note that this method will block until all locks associated with this
        manager are released.

        After destruction all created rpc object created with this loop are no
        longer valid. This method should only be called once all rpc operations
        are done.
        """
        if not self._active:
            return
        self._active = False
        for lock in self._locks:
            await lock.destroy()
        await self._loop.__aexit__(None, None, None)
        delattr(asyncio.get_running_loop(), "_zi_loop_manager")

    def create_lock(self) -> CapnpLock:
        """Create a lock that prevents the destruction of the kj event loop.

        The locks are bound to the loop manager and if locked prevent the
        destruction of the kj event loop.

        Returns:
            The created lock.
        """
        if not self._active:
            msg = (
                "The event loop in which this object was created is closed. "
                "No further operations are possible."
            )
            raise UnavailableError(msg)
        lock = CapnpLock(asyncio.Lock())
        self._locks.add(lock)
        return lock


async def ensure_capnp_event_loop() -> None:
    """Ensure that the capnp event loop is running.

    Capnp requires the capnp event loop to be running for every async
    function call to the capnp library. The lifetime of the capnp event loop
    needs to be managed. This function ensures that the capnp event loop is
    running.

    If it the capnp event loop is not running it is created and the destruction
    is scheduled at the end of the asyncio event loop. If different behavior is
    required the loop manager should be used directly. This function will
    automatically detect existing loop managers and does not create multiple
    loop managers.
    """
    if not await LoopManager.exists():
        asyncio_atexit.register((await LoopManager.create()).destroy)


async def create_lock() -> CapnpLock:
    """Create a lock for the already running event loop manager.

    The locks are bound to the loop manager and if locked prevent the
    destruction of the kj event loop.

    Returns:
        The created lock.
    """
    await ensure_capnp_event_loop()
    return LoopManager.get_running().create_lock()


def request_field_type_description(
    request: capnp.lib.capnp._Request,
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
    ) -> VectorElementType:
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
