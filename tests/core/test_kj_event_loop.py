import asyncio
from unittest.mock import patch

import labone.core.helper
import pytest
import pytest_asyncio
from labone.core.errors import LabOneCoreError, UnavailableError


@pytest_asyncio.fixture(autouse=True)
async def kj_loop():
    return


@pytest.mark.asyncio()
@patch("labone.core.helper.capnp", autospec=True)
async def test_create_event_loop_ok(capnp):
    capnp.kj_loop().__aenter__.return_value = asyncio.Future()
    capnp.kj_loop().__aexit__.return_value = asyncio.Future()
    await labone.core.helper.ensure_capnp_event_loop()
    capnp.kj_loop().__aenter__.assert_called_once()
    capnp.kj_loop().__aexit__.assert_not_called()


@patch("labone.core.helper.capnp", autospec=True)
def test_event_loop_exit_ok(capnp):
    capnp.kj_loop().__aenter__.return_value = asyncio.Future()
    capnp.kj_loop().__aexit__.return_value = asyncio.Future()
    asyncio.run(labone.core.helper.ensure_capnp_event_loop())
    capnp.kj_loop().__aenter__.assert_called_once()
    capnp.kj_loop().__aexit__.assert_called_once()


@pytest.mark.asyncio()
async def test_destroy_lock_ok():
    lock = await labone.core.helper.create_lock()
    await lock.destroy()
    with pytest.raises(UnavailableError):
        async with lock.lock():
            assert False  # noqa: PT015, B011


@pytest.mark.asyncio()
async def test_destroy_lock_multiple_times_ok():
    lock = await labone.core.helper.create_lock()
    await lock.destroy()
    await lock.destroy()
    with pytest.raises(UnavailableError):
        async with lock.lock():
            assert False  # noqa: PT015, B011


@pytest.mark.asyncio()
async def test_multiple_loop_manager_fails():
    loop_manager = await labone.core.helper.LoopManager.create()
    try:
        with pytest.raises(LabOneCoreError):
            await labone.core.helper.LoopManager.create()
    finally:
        await loop_manager.destroy()


@pytest.mark.asyncio()
async def test_destroy_loop_manager_multiple_times_ok():
    loop_manager = await labone.core.helper.LoopManager.create()
    await loop_manager.destroy()
    await loop_manager.destroy()
    with pytest.raises(UnavailableError):
        await loop_manager.create_lock()
