import asyncio
from unittest.mock import patch

import labone.core.helper
import pytest
import pytest_asyncio


@pytest_asyncio.fixture(autouse=True)
async def kj_loop():
    return


@pytest.mark.asyncio()
async def test_create_event_loop_ok():
    with patch("labone.core.helper.capnp", autospec=True) as capnp:
        capnp.kj_loop().__aenter__.return_value = asyncio.Future()
        capnp.kj_loop().__aexit__.return_value = asyncio.Future()
        await labone.core.helper.ensure_capnp_event_loop()
        capnp.kj_loop().__aenter__.assert_called_once()
        capnp.kj_loop().__aexit__.assert_not_called()


def test_event_loop_exit_ok():
    with patch("labone.core.helper.capnp", autospec=True) as capnp:
        capnp.kj_loop().__aenter__.return_value = asyncio.Future()
        capnp.kj_loop().__aexit__.return_value = asyncio.Future()
        asyncio.run(labone.core.helper.ensure_capnp_event_loop())
        capnp.kj_loop().__aenter__.assert_called_once()
        capnp.kj_loop().__aexit__.assert_called_once()
