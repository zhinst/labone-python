"""Tests for the `labone.core.subscription` module."""

import asyncio
import logging
from unittest.mock import MagicMock

import pytest

from labone.core import errors, hpk_schema
from labone.core.subscription import (
    CircularDataQueue,
    DataQueue,
    DistinctConsecutiveDataQueue,
    StreamingHandle,
)
from labone.core.value import AnnotatedValue


class FakeSubscription:
    def __init__(self):
        self.data_queues = []

    def register_data_queue(self, data_queue) -> None:
        self.data_queues.append(data_queue)


def test_data_queue_path():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", handle=subscription)
    assert queue.path == "dummy"


def test_data_queue_maxsize():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", handle=subscription)
    assert queue.maxsize == 0


def test_data_queue_maxsize_to_low():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", handle=subscription)
    queue.put_nowait("test")
    queue.put_nowait("test")
    queue.maxsize = 2
    with pytest.raises(errors.StreamingError):
        queue.maxsize = 1


def test_data_queue_maxsize_disconnected():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", handle=subscription)
    queue.disconnect()
    with pytest.raises(errors.StreamingError):
        queue.maxsize = 42


def test_data_queue_repr_idle():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", handle=subscription)
    assert repr(queue) == "DataQueue(path='dummy', maxsize=0, qsize=0, connected=True)"


def test_data_queue_repr():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", handle=subscription)
    queue.maxsize = 42
    queue.put_nowait("test")
    queue.put_nowait("test")
    queue.disconnect()
    assert (
        repr(queue) == "DataQueue(path='dummy', maxsize=42, qsize=2, connected=False)"
    )


def test_data_queue_disconnect():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", handle=subscription)
    assert queue.connected
    queue.disconnect()
    assert not queue.connected


def test_data_queue_fork():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", handle=subscription)
    assert len(subscription.data_queues) == 1
    forked_queue = queue.fork()
    assert len(subscription.data_queues) == 2
    assert forked_queue.path == queue.path
    assert forked_queue.connected


def test_data_queue_fork_disconnected():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", handle=subscription)
    queue.disconnect()
    with pytest.raises(errors.StreamingError):
        queue.fork()


def test_data_queue_put_nowait():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", handle=subscription)
    assert queue.qsize() == 0
    queue.put_nowait("test")
    assert queue.qsize() == 1
    assert queue.get_nowait() == "test"
    assert queue.qsize() == 0


def test_data_queue_put_nowait_disconnected():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", handle=subscription)
    queue.disconnect()
    with pytest.raises(errors.StreamingError):
        queue.put_nowait("test")


@pytest.mark.asyncio
async def test_data_queue_get():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", handle=subscription)
    queue.put_nowait("test")
    assert await queue.get() == "test"


@pytest.mark.asyncio
async def test_data_queue_get_timeout():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", handle=subscription)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(queue.get(), 0.01)


@pytest.mark.asyncio
async def test_data_queue_get_disconnected_ok():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", handle=subscription)
    queue.put_nowait("test")
    queue.disconnect()
    assert await queue.get() == "test"


@pytest.mark.asyncio
async def test_data_queue_get_disconnected_empty():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", handle=subscription)
    queue.disconnect()
    with pytest.raises(errors.EmptyDisconnectedDataQueueError):
        await queue.get()


@pytest.mark.asyncio
async def test_circular_data_queue_put_enough_space():
    subscription = FakeSubscription()
    queue = CircularDataQueue(
        path="dummy",
        handle=subscription,
    )
    queue.maxsize = 2
    await asyncio.wait_for(queue.put("test"), timeout=0.01)
    assert queue.qsize() == 1
    assert queue.get_nowait() == "test"


@pytest.mark.asyncio
async def test_circular_data_queue_put_full():
    subscription = FakeSubscription()
    queue = CircularDataQueue(
        path="dummy",
        handle=subscription,
    )
    queue.maxsize = 2
    await asyncio.wait_for(queue.put("test1"), timeout=0.01)
    await asyncio.wait_for(queue.put("test2"), timeout=0.01)
    await asyncio.wait_for(queue.put("test3"), timeout=0.01)
    assert queue.qsize() == 2
    assert queue.get_nowait() == "test2"
    assert queue.get_nowait() == "test3"


@pytest.mark.asyncio
async def test_circular_data_queue_put_no_wait_enough_space():
    subscription = FakeSubscription()
    queue = CircularDataQueue(
        path="dummy",
        handle=subscription,
    )
    queue.maxsize = 2
    queue.put_nowait("test")
    assert queue.qsize() == 1
    assert queue.get_nowait() == "test"


@pytest.mark.asyncio
async def test_circular_data_queue_put_no_wait_full():
    subscription = FakeSubscription()
    queue = CircularDataQueue(
        path="dummy",
        handle=subscription,
    )
    queue.maxsize = 2
    queue.put_nowait("test1")
    queue.put_nowait("test2")
    queue.put_nowait("test3")
    assert queue.qsize() == 2
    assert queue.get_nowait() == "test2"
    assert queue.get_nowait() == "test3"


def test_circular_data_queue_fork():
    subscription = FakeSubscription()
    queue = CircularDataQueue(
        path="dummy",
        handle=subscription,
    )
    assert len(subscription.data_queues) == 1
    forked_queue = queue.fork()
    assert isinstance(forked_queue, CircularDataQueue)
    assert len(subscription.data_queues) == 2
    assert forked_queue.path == queue.path
    assert forked_queue.connected


def test_streaming_handle_register():
    streaming_handle = StreamingHandle()
    DataQueue(path="dummy", handle=streaming_handle)
    assert len(streaming_handle._data_queues) == 1


@pytest.mark.parametrize("num_values", range(0, 20, 4))
@pytest.mark.parametrize("num_queues", [1, 2, 6])
@pytest.mark.asyncio
async def test_streaming_handle_update_event(num_values, num_queues):
    streaming_handle = StreamingHandle()
    queues = []
    for _ in range(num_queues):
        queue = DataQueue(
            path="dummy",
            handle=streaming_handle,
        )
        queues.append(queue)
    for i in range(num_values):
        value = AnnotatedValue(value=i, path="dummy", timestamp=0)
        streaming_handle.distribute_to_data_queues(value)
    for queue in queues:
        assert queue.qsize() == num_values
        for i in range(num_values):
            assert queue.get_nowait() == AnnotatedValue(
                value=i,
                path="dummy",
                timestamp=0,
            )


def test_streaming_handle_with_parser_callback():
    StreamingHandle(
        parser_callback=lambda a: AnnotatedValue(path=a.path, value=a.value * 2),
    )


@pytest.mark.asyncio
async def test_capnp_callback(caplog):
    streaming_handle = StreamingHandle()
    queue = DataQueue(
        path="dummy",
        handle=streaming_handle,
    )
    call_param = hpk_schema.StreamingHandleSendValuesParams()
    values = call_param.init_values(2)

    values[0].init_metadata(timestamp=0, path="dummy")
    values[0].init_value(int64=42)

    values[1].init_metadata(timestamp=1, path="dummy")
    values[1].init_value(double=22.0)

    fulfiller = MagicMock()
    with caplog.at_level(logging.ERROR):
        await streaming_handle.capnp_callback(0, 0, call_param, fulfiller)
    assert "" in caplog.text
    assert queue.qsize() == 2
    assert queue.get_nowait() == AnnotatedValue(value=42, path="dummy", timestamp=0)
    assert queue.get_nowait() == AnnotatedValue(value=22.0, path="dummy", timestamp=1)
    fulfiller.fulfill.assert_called_once()


@pytest.mark.asyncio
async def test_streaming_error(caplog):
    streaming_handle = StreamingHandle()
    queue = DataQueue(
        path="dummy",
        handle=streaming_handle,
    )
    call_param = hpk_schema.StreamingHandleSendValuesParams()
    values = call_param.init_values(1)
    values[0].init_metadata(timestamp=0, path="dummy")
    values[0].init_value().init_streamingError(
        code=1,
        message="test error",
        category="unknown",
    )
    fulfiller = MagicMock()
    with caplog.at_level(logging.ERROR):
        await streaming_handle.capnp_callback(0, 0, call_param, fulfiller)
    assert "test error" in caplog.text
    assert queue.qsize() == 0
    fulfiller.fulfill.assert_called_once()


@pytest.mark.asyncio
async def test_streaming_error_with_value(caplog):
    streaming_handle = StreamingHandle()
    queue = DataQueue(
        path="dummy",
        handle=streaming_handle,
    )
    call_param = hpk_schema.StreamingHandleSendValuesParams()
    values = call_param.init_values(2)

    # Fist value is a streaming error
    values[0].init_metadata(timestamp=0, path="dummy")
    values[0].init_value().init_streamingError(
        code=1,
        message="test error",
        category="unknown",
    )

    # Second value is a normal value
    values[1].init_metadata(timestamp=0, path="dummy")
    values[1].init_value(int64=42)

    fulfiller = MagicMock()
    with caplog.at_level(logging.ERROR):
        await streaming_handle.capnp_callback(0, 0, call_param, fulfiller)
    assert "test error" in caplog.text
    assert queue.qsize() == 1
    assert queue.get_nowait() == AnnotatedValue(value=42, path="dummy", timestamp=0)
    fulfiller.fulfill.assert_called_once()


@pytest.mark.asyncio
async def test_distinct_data_queue_put_no_wait_new_value():
    subscription = FakeSubscription()
    queue = DistinctConsecutiveDataQueue(
        path="dummy",
        handle=subscription,
    )
    value1 = AnnotatedValue(value=1, path="dummy")
    value2 = AnnotatedValue(value=2, path="dummy")
    queue.put_nowait(value1)
    queue.put_nowait(value2)
    assert queue.qsize() == 2
    assert queue.get_nowait() == value1
    assert queue.get_nowait() == value2


@pytest.mark.asyncio
async def test_distinct_data_queue_put_no_wait_same_value():
    subscription = FakeSubscription()
    queue = DistinctConsecutiveDataQueue(
        path="dummy",
        handle=subscription,
    )
    value = AnnotatedValue(value=1, path="dummy")
    queue.put_nowait(value)
    queue.put_nowait(value)
    assert queue.qsize() == 1
    assert queue.get_nowait() == value


def test_distinct_data_queue_fork():
    subscription = FakeSubscription()
    queue = DistinctConsecutiveDataQueue(
        path="dummy",
        handle=subscription,
    )
    assert len(subscription.data_queues) == 1
    forked_queue = queue.fork()
    assert isinstance(forked_queue, DistinctConsecutiveDataQueue)
    assert len(subscription.data_queues) == 2
    assert forked_queue.path == queue.path
    assert forked_queue.connected
