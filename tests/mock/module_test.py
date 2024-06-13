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

from labone.core import AnnotatedValue
from labone.core.shf_vector_data import (
    SHFDemodSample,
    ShfDemodulatorVectorExtraHeader,
    ShfResultLoggerVectorExtraHeader,
    ShfScopeVectorExtraHeader,
)
from labone.mock import AutomaticLabOneServer


@pytest.mark.asyncio()
async def test_useable_via_entry_point():
    """If this crashes, the module is not useable in the desired manner.
    Tests that a session can be established and used.
    Only looks that no error is raised.
    """
    session = await AutomaticLabOneServer({"/a/b": {}}).start_pipe()
    await session.set(AnnotatedValue(path="/a/b", value=7))


@pytest.mark.asyncio()
async def test_subscription():
    session = await AutomaticLabOneServer({"/a/b": {}}).start_pipe()

    queue = await session.subscribe("/a/b")
    await session.set(AnnotatedValue(path="/a/b", value=7))
    assert (await queue.get()).value == 7
    assert queue.empty()


@pytest.mark.asyncio()
async def test_subscription_multiple_changes():
    session = await AutomaticLabOneServer({"/a/b": {}}).start_pipe()

    queue = await session.subscribe("/a/b")
    await session.set(AnnotatedValue(path="/a/b", value=7))
    await session.set(AnnotatedValue(path="/a/b", value=3))
    await session.set(AnnotatedValue(path="/a/b", value=5))
    assert (await queue.get()).value == 7
    assert (await queue.get()).value == 3
    assert (await queue.get()).value == 5
    assert queue.empty()


@pytest.mark.asyncio()
async def test_subscription_seperate_for_each_path():
    session = await AutomaticLabOneServer({"/a/b": {}, "/a/c": {}}).start_pipe()

    queue = await session.subscribe("/a/b")
    queue2 = await session.subscribe("/a/c")
    await session.set(AnnotatedValue(path="/a/b", value=7))
    await session.set(AnnotatedValue(path="/a/c", value=5))
    assert (await queue.get()).value == 7
    assert (await queue2.get()).value == 5
    assert queue.empty()
    assert queue2.empty()


@pytest.mark.asyncio()
async def test_subscription_updated_by_set_with_expression():
    session = await AutomaticLabOneServer({"/a/b": {}}).start_pipe()

    queue = await session.subscribe("/a/b")
    await session.set_with_expression(AnnotatedValue(path="/a", value=7))
    assert (await queue.get()).value == 7
    assert queue.empty()


@pytest.mark.asyncio()
async def test_shf_scope_vector_handled_correctly_through_set_and_subscription():
    value = np.array([6 + 6j, 3 + 3j], dtype=np.complex64)
    extra_header = ShfScopeVectorExtraHeader(
        0,
        0,
        False,  # noqa: FBT003
        3.0,
        7,
        0,
        0,
        1,
        1,
        1,
        1,
        0,
    )

    session = await AutomaticLabOneServer({"/a/b": {}}).start_pipe()

    queue = await session.subscribe("/a/b")
    await session.mock_server.set(
        AnnotatedValue(
            path="/a/b",
            value=value.copy(),
            timestamp=0,
            extra_header=extra_header,
        ),
    )
    assert list((await queue.get()).value) == list(value)


@pytest.mark.asyncio()
async def test_shf_result_logger_vector_handled_correctly_in_set_and_subscribe():
    value = np.array([50 + 100j, 100 + 150j], dtype=np.complex64)
    extra_header = ShfResultLoggerVectorExtraHeader(
        1,
        2,
        3,
        50,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
    )

    session = await AutomaticLabOneServer({"/a/b": {}}).start_pipe()

    queue = await session.subscribe("/a/b")
    await session.mock_server.set(
        AnnotatedValue(
            path="/a/b",
            value=value.copy(),
            timestamp=0,
            extra_header=extra_header,
        ),
    )
    assert list((await queue.get()).value) == list(value)


@pytest.mark.asyncio()
async def test_shf_demodulator_vector_handled_correctly_through_set_and_subscription():
    value = SHFDemodSample(
        np.array([6, 3], dtype=np.int64),
        np.array([7, 2], dtype=np.int64),
    )
    extra_header = ShfDemodulatorVectorExtraHeader(
        timestamp=0,
        timestamp_delta=0,
        burst_length=4,
        burst_offset=5,
        trigger_index=6,
        trigger_timestamp=7,
        center_freq=8,
        rf_path=True,
        oscillator_source=3,
        harmonic=10,
        trigger_source=2,
        signal_source=4,
        oscillator_freq=13,
        scaling=4.0000000467443897e-07,
    )

    session = await AutomaticLabOneServer({"/a/b": {}}).start_pipe()

    queue = await session.subscribe("/a/b")
    await session.mock_server.set(
        AnnotatedValue(
            path="/a/b",
            value=SHFDemodSample(x=value.x.copy(), y=value.y.copy()),
            timestamp=0,
            extra_header=extra_header,
        ),
    )
    subscription_value = await queue.get()
    assert np.allclose(subscription_value.value.x, value.x)
    assert np.allclose(subscription_value.value.y, value.y)


@pytest.mark.asyncio()
async def test_ensure_compatibility():
    session = await AutomaticLabOneServer({}).start_pipe()
    await session.ensure_compatibility()
