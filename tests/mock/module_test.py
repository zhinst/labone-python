"""
Scope: session, capnp (without localhost, but within python),
        server, concrete server, functionality

Subscription behavior can only be tested with a client, holding a queue and
a mock server on the other side of capnp. Aiming to test the
AutomaticLabOneServer,
we still need to use a larger scope in order to test meaningful behavior.

"""

import numpy as np
import pytest

from labone.core import hpk_schema
from labone.core.shf_vector_data import (
    ShfDemodulatorVectorData,
    ShfResultLoggerVectorData,
    ShfScopeVectorData,
)
from labone.mock import AutomaticLabOneServer


@pytest.mark.asyncio
async def test_useable_via_entry_point():
    """If this crashes, the module is not useable in the desired manner.
    Tests that a session can be established and used.
    Only looks that no error is raised.
    """
    session = await AutomaticLabOneServer({"/a/b": {}}).start_pipe()
    await session.set(value=7, path="/a/b")


@pytest.mark.asyncio
async def test_subscription():
    session = await AutomaticLabOneServer({"/a/b": {}}).start_pipe()

    queue = await session.subscribe("/a/b")
    await session.set(path="/a/b", value=7)
    assert (await queue.get()).value == 7
    assert queue.empty()


@pytest.mark.asyncio
async def test_unsubscribe():
    session = await AutomaticLabOneServer({"/a/b": {}}).start_pipe()

    queue = await session.subscribe("/a/b")
    queue.disconnect()
    await session.set(path="/a/b", value=7)
    assert queue.empty()


@pytest.mark.asyncio
async def test_subscription_multiple_changes():
    session = await AutomaticLabOneServer({"/a/b": {}}).start_pipe()

    queue = await session.subscribe("/a/b")
    await session.set(path="/a/b", value=7)
    await session.set(path="/a/b", value=3)
    await session.set(path="/a/b", value=5)
    assert (await queue.get()).value == 7
    assert (await queue.get()).value == 3
    assert (await queue.get()).value == 5
    assert queue.empty()


@pytest.mark.asyncio
async def test_subscription_seperate_for_each_path():
    session = await AutomaticLabOneServer({"/a/b": {}, "/a/c": {}}).start_pipe()

    queue = await session.subscribe("/a/b")
    queue2 = await session.subscribe("/a/c")
    await session.set(path="/a/b", value=7)
    await session.set(path="/a/c", value=5)
    assert (await queue.get()).value == 7
    assert (await queue2.get()).value == 5
    assert queue.empty()
    assert queue2.empty()


@pytest.mark.asyncio
async def test_subscription_updated_by_set_with_expression():
    session = await AutomaticLabOneServer({"/a/b": {}}).start_pipe()

    queue = await session.subscribe("/a/b")
    await session.set_with_expression(path="/a", value=7)
    assert (await queue.get()).value == 7
    assert queue.empty()


@pytest.mark.asyncio
async def test_shf_scope_vector_handled_correctly_through_set_and_subscription():
    value = ShfScopeVectorData(
        vector=np.array([6 + 6j, 3 + 3j], dtype=np.complex128),
        properties=hpk_schema.ShfScopeVectorData().properties,
    )
    value.properties.scaling = 3.0
    value.properties.centerFrequency = 7

    session = await AutomaticLabOneServer({"/a/b": {}}).start_pipe()

    queue = await session.subscribe("/a/b")
    await session.mock_server.set(path="/a/b", value=value)
    result = await queue.get()
    assert np.allclose(result.value.vector, value.vector)


@pytest.mark.asyncio
async def test_shf_result_logger_vector_handled_correctly_in_set_and_subscribe():
    value = ShfResultLoggerVectorData(
        vector=np.array([50 + 100j, 100 + 150j], dtype=np.complex128),
        properties=hpk_schema.ShfResultLoggerVectorData().properties,
    )
    value.properties.scaling = 3.0
    value.properties.centerFrequency = 7

    session = await AutomaticLabOneServer({"/a/b": {}}).start_pipe()

    queue = await session.subscribe("/a/b")
    await session.mock_server.set(path="/a/b", value=value)
    result = await queue.get()
    assert np.allclose(result.value.vector, value.vector)


@pytest.mark.asyncio
async def test_shf_demodulator_vector_handled_correctly_through_set_and_subscription():
    value = ShfDemodulatorVectorData(
        x=np.array([6, 3], dtype=np.int64),
        y=np.array([7, 2], dtype=np.int64),
        properties=hpk_schema.ShfDemodulatorVectorData().properties,
    )
    session = await AutomaticLabOneServer({"/a/b": {}}).start_pipe()

    queue = await session.subscribe("/a/b")
    await session.mock_server.set(path="/a/b", value=value)
    subscription_value = await queue.get()
    assert np.allclose(subscription_value.value.x, value.x)
    assert np.allclose(subscription_value.value.y, value.y)


@pytest.mark.asyncio
async def test_ensure_compatibility():
    session = await AutomaticLabOneServer({}).start_pipe()
    session.ensure_compatibility()
