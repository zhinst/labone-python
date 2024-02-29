from typing import Any

import pytest
import pytest_asyncio
from labone.core.helper import LoopManager

from .resources import (
    error_capnp,
    path_capnp,
    result_capnp,
    session_protocol_capnp,
    uuid_capnp,
    value_capnp,
)


@pytest_asyncio.fixture(autouse=True)
async def kj_loop():
    """Ensures that the capnp event loop is running.

    Its important that every test creates and closes the kj_event loop.
    This helps to avoid leaking errors and promises from one test to another.
    """
    loop_manager = await LoopManager.create()
    try:
        yield
    finally:
        await loop_manager.destroy()


class MockReflectionServer:
    def __init__(self):
        self._loaded_nodes = {}
        self._available_schema = [
            error_capnp,
            path_capnp,
            result_capnp,
            session_protocol_capnp,
            uuid_capnp,
            value_capnp,
        ]

    def __getattr__(self, name: str) -> Any:
        for schema in self._available_schema:
            if hasattr(schema, name):
                return getattr(schema, name)
        msg = f"MockReflectionServer has no attribute {name}"
        raise AttributeError(msg)


@pytest.fixture()
def reflection_server():
    """Returns a reflection server instance."""
    return MockReflectionServer()
