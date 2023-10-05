import capnp
import pytest_asyncio


@pytest_asyncio.fixture(autouse=True)
async def kj_loop():
    """Ensures that the capnp event loop is running.

    Its important that every test creates and closes the kj_event loop.
    This helps to avoid leaking errors and promises from one test to another.
    """
    async with capnp.kj_loop():
        yield
