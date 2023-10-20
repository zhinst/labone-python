"""Tests for the `labone.core.subscription` module."""
import asyncio

import capnp
import pytest
from labone.core import errors
from labone.core.resources import session_protocol_capnp
from labone.core.subscription import DataQueue, StreamingHandle
from labone.core.value import AnnotatedValue


class FakeSubscription:
    def __init__(self):
        self.data_queues = []

    def register_data_queue(self, data_queue) -> None:
        self.data_queues.append(data_queue)


def test_data_queue_path():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", register_function=subscription.register_data_queue)
    assert queue.path == "dummy"


def test_data_queue_maxsize():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", register_function=subscription.register_data_queue)
    assert queue.maxsize == 0


def test_data_queue_maxsize_to_low():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", register_function=subscription.register_data_queue)
    queue.put_nowait("test")
    queue.maxsize = 2
    with pytest.raises(errors.StreamingError):
        queue.maxsize = 1


def test_data_queue_maxsize_disconnected():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", register_function=subscription.register_data_queue)
    queue.disconnect()
    with pytest.raises(errors.StreamingError):
        queue.maxsize = 42


def test_data_queue_repr_idle():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", register_function=subscription.register_data_queue)
    assert repr(queue) == "DataQueue(path='dummy', maxsize=0, qsize=0, connected=True)"


def test_data_queue_repr():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", register_function=subscription.register_data_queue)
    queue.maxsize = 42
    queue.put_nowait("test")
    queue.put_nowait("test")
    queue.disconnect()
    assert (
        repr(queue) == "DataQueue(path='dummy', maxsize=42, qsize=2, connected=False)"
    )


def test_data_queue_disconnect():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", register_function=subscription.register_data_queue)
    assert queue.connected
    queue.disconnect()
    assert not queue.connected


def test_data_queue_fork():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", register_function=subscription.register_data_queue)
    assert len(subscription.data_queues) == 1
    forked_queue = queue.fork()
    assert len(subscription.data_queues) == 2
    assert forked_queue.path == queue.path
    assert forked_queue.connected


def test_data_queue_fork_disconnected():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", register_function=subscription.register_data_queue)
    queue.disconnect()
    with pytest.raises(errors.StreamingError):
        queue.fork()


def test_data_queue_put_nowait():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", register_function=subscription.register_data_queue)
    assert queue.qsize() == 0
    queue.put_nowait("test")
    assert queue.qsize() == 1
    assert queue.get_nowait() == "test"
    assert queue.qsize() == 0


def test_data_queue_put_nowait_disconnected():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", register_function=subscription.register_data_queue)
    queue.disconnect()
    with pytest.raises(errors.StreamingError):
        queue.put_nowait("test")


@pytest.mark.asyncio()
async def test_data_queue_get():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", register_function=subscription.register_data_queue)
    queue.put_nowait("test")
    assert await queue.get() == "test"


@pytest.mark.asyncio()
async def test_data_queue_get_timeout():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", register_function=subscription.register_data_queue)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(queue.get(), 0.01)


@pytest.mark.asyncio()
async def test_data_queue_get_disconnected_ok():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", register_function=subscription.register_data_queue)
    queue.put_nowait("test")
    queue.disconnect()
    assert await queue.get() == "test"


@pytest.mark.asyncio()
async def test_data_queue_get_disconnected_empty():
    subscription = FakeSubscription()
    queue = DataQueue(path="dummy", register_function=subscription.register_data_queue)
    queue.disconnect()
    with pytest.raises(errors.EmptyDisconnectedDataQueueError):
        await queue.get()


def test_streaming_handle_register():
    streaming_handle = StreamingHandle()
    DataQueue(path="dummy", register_function=streaming_handle.register_data_queue)
    assert len(streaming_handle._data_queues) == 1


@pytest.mark.parametrize("num_values", range(0, 20, 4))
@pytest.mark.parametrize("num_queues", [1, 2, 6])
@pytest.mark.asyncio()
async def test_streaming_handle_update_event(num_values, num_queues):
    streaming_handle = StreamingHandle()
    queues = []
    for _ in range(num_queues):
        queue = DataQueue(
            path="dummy",
            register_function=streaming_handle.register_data_queue,
        )
        queues.append(queue)
    values = []
    for i in range(num_values):
        value = session_protocol_capnp.AnnotatedValue.new_message()
        value.metadata.path = "dummy"
        value.value.int64 = i
        values.append(value)
    await streaming_handle.sendValues(values)
    for queue in queues:
        assert queue.qsize() == num_values
        for i in range(num_values):
            assert queue.get_nowait() == AnnotatedValue(
                value=i,
                path="dummy",
                timestamp=0,
                extra_header=None,
            )


def test_streaming_handle_with_parser_callback():
    StreamingHandle(
        parser_callback=lambda a: AnnotatedValue(path=a.path, value=a.value * 2),
    )


@pytest.mark.asyncio()
async def test_streaming_handle_update_empty():
    streaming_handle = StreamingHandle()
    values = []
    value = session_protocol_capnp.AnnotatedValue.new_message()
    values.append(value)
    with pytest.raises(capnp.KjException):
        await streaming_handle.sendValues(values)


@pytest.mark.asyncio()
async def test_streaming_handle_update_disconnect():
    streaming_handle = StreamingHandle()
    queue = DataQueue(
        path="dummy",
        register_function=streaming_handle.register_data_queue,
    )
    queue.disconnect()
    values = []
    value = session_protocol_capnp.AnnotatedValue.new_message()
    values.append(value)
    with pytest.raises(capnp.KjException):
        await streaming_handle.sendValues(values)


@pytest.mark.asyncio()
async def test_streaming_handle_update_partially_disconnected():
    streaming_handle = StreamingHandle()
    queue_0 = DataQueue(
        path="dummy",
        register_function=streaming_handle.register_data_queue,
    )
    queue_1 = DataQueue(
        path="dummy",
        register_function=streaming_handle.register_data_queue,
    )
    queue_0.disconnect()
    values = []
    value = session_protocol_capnp.AnnotatedValue.new_message()
    value.metadata.path = "dummy"
    value.value.int64 = 1
    values.append(value)
    await streaming_handle.sendValues(values)
    assert queue_0.qsize() == 0
    assert queue_1.qsize() == 1
    assert queue_1.get_nowait() == AnnotatedValue(
        value=1,
        path="dummy",
        timestamp=0,
        extra_header=None,
    )
    queue_1.disconnect()
    with pytest.raises(capnp.KjException):
        await streaming_handle.sendValues(values)


@pytest.mark.asyncio()
async def test_streaming_handle_update_queue_full_single():
    streaming_handle = StreamingHandle()
    queue_0 = DataQueue(
        path="dummy",
        register_function=streaming_handle.register_data_queue,
    )
    queue_1 = DataQueue(
        path="dummy",
        register_function=streaming_handle.register_data_queue,
    )
    queue_0.maxsize = 1
    queue_0.put_nowait("dummy")
    assert queue_0.qsize() == 1
    values = []
    value = session_protocol_capnp.AnnotatedValue.new_message()
    value.metadata.path = "dummy"
    value.value.int64 = 1
    values.append(value)
    await streaming_handle.sendValues(values)
    assert queue_0.qsize() == 1
    assert queue_1.qsize() == 1
    assert queue_1.get_nowait() == AnnotatedValue(
        value=1,
        path="dummy",
        timestamp=0,
        extra_header=None,
    )


@pytest.mark.asyncio()
async def test_streaming_handle_update_queue_full_multiple():
    streaming_handle = StreamingHandle()
    queue_0 = DataQueue(
        path="dummy",
        register_function=streaming_handle.register_data_queue,
    )
    queue_1 = DataQueue(
        path="dummy",
        register_function=streaming_handle.register_data_queue,
    )
    queue_0.maxsize = 1
    queue_0.put_nowait("dummy")
    queue_1.maxsize = 1
    queue_1.put_nowait("dummy")
    values = []
    value = session_protocol_capnp.AnnotatedValue.new_message()
    value.metadata.path = "dummy"
    value.value.int64 = 1
    values.append(value)
    with pytest.raises(capnp.KjException):
        await streaming_handle.sendValues(values)
