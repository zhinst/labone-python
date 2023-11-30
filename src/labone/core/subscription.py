"""This module contains the logic for the subscription mechanism.

Subscriptions are implemented through the capnp stream mechanism. This handles
all the communication stuff, e.g. back pressure and flow control. The only thing
the client needs to provide is a `SessionClient.StreamingHandle.Server`
implementation. The `sendValues()` method of this class will be called by the
kernel through RPC whenever an update event for the subscribed node is
received. To make this as simple as possible the user only interacts with
a DataQueue object. A async queue with the addition of a connection guard.

It is possible to create a fork of a data queue. A fork will receive the
values from the same underlying subscription as the original data queue.
However, the connection state of the fork is independent of the original.

It is always recommended to disconnect the data queue when it is not needed
anymore. This will free up resources on the server side and prevent the server
from sending unnecessary data.
"""
from __future__ import annotations

import asyncio
import logging
import typing as t
import weakref
from abc import ABC, abstractmethod

import capnp

from labone.core import errors
from labone.core.value import AnnotatedValue

if t.TYPE_CHECKING:
    from labone.core.helper import CapnpCapability, LabOneNodePath
    from labone.core.reflection.server import ReflectionServer

logger = logging.getLogger(__name__)


class _ConnectionState:
    """Connection state guard.

    Helper class that represents the connection state. The sole purpose of this
    class is to have an expressive way of showing that a disconnect on a data
    queue is final and can not be reverted.
    """

    def __init__(self) -> None:
        self.__connected = True

    @property
    def connected(self) -> bool:
        """Connection state."""
        return self.__connected

    def disconnect(self) -> None:
        """Disconnect the data queue.

        This operation is final and can not be reverted.
        """
        self.__connected = False

    def __bool__(self) -> bool:
        """All."""
        return self.connected


class DataQueue(asyncio.Queue):
    """Queue for a single node subscription.

    The Queue holds all values updates received for the subscribed node. This
    interface is identical to the asyncio.Queue interface, with a additional
    connection guard. If the data queue is disconnected, the subscription will
    eventually be canceled on the kernel side. In any case a disconnected data
    queue will not receive any new values.

    Warning:
        The disconnect will only be recognized by the kernel/server during the
        next update event. Until that the server will not be aware of the
        disconnect. (e.g. asking the kernel which nodes are subscribed might not
        reflect the reality).

    Args:
        path: Path of the subscribed node.
    """

    def __init__(
        self,
        *,
        path: LabOneNodePath,
        register_function: t.Callable[[weakref.ReferenceType[DataQueue]], None],
    ) -> None:
        super().__init__()
        self._path = path
        self._connection_state = _ConnectionState()
        self._register_function = register_function

        register_function(weakref.ref(self))

    def __repr__(self) -> str:
        return str(
            f"{self.__class__.__name__}(path={self._path!r}, "
            f"maxsize={self.maxsize}, qsize={self.qsize()}, "
            f"connected={self.connected})",
        )

    @t.overload
    def fork(self, queue_type: None) -> DataQueue:
        ...

    @t.overload
    def fork(
        self,
        queue_type: type[QueueProtocol],
    ) -> QueueProtocol:
        ...

    def fork(
        self,
        queue_type: type[QueueProtocol] | None = None,
    ) -> DataQueue | QueueProtocol:
        """Create a fork of the subscription.

        The forked subscription will receive all updates that the original
        subscription receives. Its connection state is independent of the original
        subscription, meaning even if the original subscription is disconnected,
        the forked subscription will still receive updates.

        Warning:
            The forked subscription will not contain any values before the fork.

        Args:
            queue_type: The type of the queue to be returned. This can be
                any class matching the DataQueue interface. Only needed if the
                default DataQueue class is not sufficient. If None is passed
                the default DataQueue class is used. (default=None)

        Returns:
            A new data queue to the same underlying subscription.
        """
        if not self._connection_state:
            msg = str(
                "The data queue has been disconnected. A fork does not make "
                "sense as it would never receive data.",
            )
            raise errors.StreamingError(msg)
        new_queue_type = queue_type or DataQueue
        return new_queue_type(
            path=self._path,
            register_function=self._register_function,
        )

    def disconnect(self) -> None:
        """Disconnect the data queue.

        This operation is final and can not be reverted. A disconnected queue
        will not receive any new values.

        Important:
            It is always recommended to disconnect the data queue when it is not
            needed anymore. This will free up resources on the server side and
            prevent the server from sending unnecessary data.
        """
        self._connection_state.disconnect()

    def put_nowait(self, item: AnnotatedValue) -> None:
        """Put an item into the queue without blocking.

        Args:
            item: The item to the put in the queue.

        Raises:
            StreamingError: If the data queue has been disconnected.
        """
        if not self._connection_state:
            msg = "The data queue has been disconnected."
            raise errors.StreamingError(msg)
        return super().put_nowait(item)

    async def get(self) -> AnnotatedValue:
        """Remove and return an item from the queue.

        Returns:
            The first item in the queue. If the queue is empty, wait until an
            item is available.

        Raises:
            EmptyDisconnectedDataQueueError: If the data queue if empty AND
            disconnected.
        """
        if self.empty() and not self._connection_state:
            msg = str(
                "The data queue is empty and it has been disconnected, "
                "therefore it will not receive data anymore.",
            )
            raise errors.EmptyDisconnectedDataQueueError(
                msg,
            )
        return await super().get()

    @property
    def connected(self) -> bool:
        """Connection state."""
        return bool(self._connection_state)

    @property
    def path(self) -> LabOneNodePath:
        """Path of the subscribed node."""
        return self._path

    @property
    def maxsize(self) -> int:
        """Number of items allowed in the queue."""
        return self._maxsize

    @maxsize.setter
    def maxsize(self, maxsize: int) -> None:
        """Number of items allowed in the queue."""
        if not self._connection_state:
            msg = str(
                "Has been disconnected, therefore it will not receive data anymore."
                "Changing the maxsize will not have any effect.",
            )
            raise errors.StreamingError(msg)
        if self.qsize() > maxsize:
            msg = str(
                "The new maxsize is smaller than the current qsize. "
                "This results in data loss and is forbidden.",
            )
            raise errors.StreamingError(msg)
        self._maxsize = maxsize


QueueProtocol = t.TypeVar("QueueProtocol", bound=DataQueue)


class CircularDataQueue(DataQueue):
    """Circular data queue.

    This data queue is identical to the DataQueue, with the exception that it
    will remove the oldest item from the queue if the queue is full and a new
    item is added.
    """

    async def put(self, item: AnnotatedValue) -> None:
        """Put an item into the queue.

        If the queue is full the oldest item will be removed and the new item
        will be added to the end of the queue.

        Args:
            item: The item to the put in the queue.

        Raises:
            StreamingError: If the data queue has been disconnected.
        """
        if self.full():
            self.get_nowait()
        await super().put(item)

    def put_nowait(self, item: AnnotatedValue) -> None:
        """Put an item into the queue without blocking.

        If the queue is full the oldest item will be removed and the new item
        will be added to the end of the queue.

        Args:
            item: The item to the put in the queue.

        Raises:
            StreamingError: If the data queue has been disconnected.
        """
        if self.full():
            self.get_nowait()
        super().put_nowait(item)

    @t.overload
    def fork(self, queue_type: None) -> CircularDataQueue:
        ...

    @t.overload
    def fork(
        self,
        queue_type: type[QueueProtocol],
    ) -> QueueProtocol:
        ...

    def fork(
        self,
        queue_type: type[QueueProtocol] | None = None,
    ) -> CircularDataQueue | QueueProtocol:
        """Create a fork of the subscription.

        The forked subscription will receive all updates that the original
        subscription receives. Its connection state is independent of the original
        subscription, meaning even if the original subscription is disconnected,
        the forked subscription will still receive updates.

        Warning:
            The forked subscription will not contain any values before the fork.

        Args:
            queue_type: The type of the queue to be returned. This can be
                any class matching the DataQueue interface. Only needed if the
                default DataQueue class is not sufficient. If None is passed
                the default DataQueue class is used. (default=None)

        Returns:
            A new data queue to the same underlying subscription.
        """
        return DataQueue.fork(
            self,
            queue_type=queue_type if queue_type is not None else CircularDataQueue,
        )


class DistinctConsecutiveDataQueue(DataQueue):
    """Data queue that only accepts values which have changed.

    This data queue is identical to the DataQueue, with the exception that it
    will accept new values that have a different value than the last value.

    Args:
        path: Path of the subscribed node.
        register_function: Function to register this queue in the underlying
            subscription handle.
    """

    def __init__(
        self,
        *,
        path: LabOneNodePath,
        register_function: t.Callable[[weakref.ReferenceType[DataQueue]], None],
    ) -> None:
        DataQueue.__init__(
            self,
            path=path,
            register_function=register_function,
        )
        self._last_value = AnnotatedValue(value=None, path="unkown")

    def put_nowait(self, item: AnnotatedValue) -> None:
        """Put an item into the queue without blocking.

        If the queue is full the oldest item will be removed and the new item
        will be added to the end of the queue.

        Args:
            item: The item to the put in the queue.

        Raises:
            StreamingError: If the data queue has been disconnected.
        """
        if item.value != self._last_value.value:
            DataQueue.put_nowait(self, item)
            self._last_value = item

    @t.overload
    def fork(self, queue_type: None) -> CircularDataQueue:
        ...

    @t.overload
    def fork(
        self,
        queue_type: type[QueueProtocol],
    ) -> QueueProtocol:
        ...

    def fork(
        self,
        queue_type: type[QueueProtocol] | None = None,
    ) -> CircularDataQueue | QueueProtocol:
        """Create a fork of the subscription.

        The forked subscription will receive all updates that the original
        subscription receives. Its connection state is independent of the original
        subscription, meaning even if the original subscription is disconnected,
        the forked subscription will still receive updates.

        Warning:
            The forked subscription will not contain any values before the fork.

        Args:
            queue_type: The type of the queue to be returned. This can be
                any class matching the DataQueue interface. Only needed if the
                default DataQueue class is not sufficient. If None is passed
                the default DataQueue class is used. (default=None)

        Returns:
            A new data queue to the same underlying subscription.
        """
        return DataQueue.fork(
            self,
            queue_type=queue_type
            if queue_type is not None
            else DistinctConsecutiveDataQueue,
        )


class StreamingHandle(ABC):
    """Streaming Handle server.

    This class is passed to the kernel when a subscription is requested.
    Every update event to the subscribed node will result in the kernel
    calling the sendValues method.

    Warning:
        This function is owned by capnp and should not be called or referenced
        by the user.

    The StreamingHandle holds only a weak reference to the data queue to which
    the values will be added. This is done to avoid the subscription to stay
    alive even if no one hold a reference to the data queue.

    Args:
        data_queue: Weak reference to the data queue to which the values
            will be added.
    """

    @abstractmethod
    def __init__(
        self,
        *,
        parser_callback: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
    ) -> None:
        ...

    @abstractmethod
    def register_data_queue(
        self,
        data_queue: weakref.ReferenceType[QueueProtocol],
    ) -> None:
        """Register a new data queue.

        Args:
            data_queue: Weak reference to the data queue to which the values
                will be added.
        """
        ...

    @abstractmethod
    async def sendValues(  # noqa: N802 (function name is enforced through the schema)
        self,
        values: CapnpCapability,
        **_,
    ) -> None:
        """Capnp Interface callback.

        This function is called by the kernel (through RPC) when an update
        event for the subscribed node is received.

        Args:
            values: List of update events for the subscribed node.

        Raises:
            capnp.KjException: If no data queues are registered any more and
                the subscription should be removed.
        """
        ...


def streaming_handle_factory(
    reflection_server: ReflectionServer,
) -> type[StreamingHandle]:
    """Factory for the StreamingHandle class.

    The StreamingHandle class is a capnp server implementation. It is passed
    to the server when a subscription is requested. Every update event to the
    subscription will result in the server calling the sendValues method.

    Since due to the dynamic reflection mechanism (loading capnp schema at
    runtime) used, the StreamingHandle class can not be defined statically.

    Args:
        reflection_server: The reflection server that is used for the session.

    Returns:
        The StreamingHandle class, matching the schema of the reflection server.
    """

    class StreamingHandleImpl(
        StreamingHandle,
        reflection_server.StreamingHandle.Server,  # type: ignore[name-defined]
    ):
        """Streaming Handle server implementation.

        Args:
            data_queue: Weak reference to the data queue to which the values
                will be added.
        """

        def __init__(
            self,
            *,
            parser_callback: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
        ) -> None:
            self._data_queues = []  # type: ignore[var-annotated]

            if parser_callback is None:

                def parser_callback(x: AnnotatedValue) -> AnnotatedValue:
                    return x

            self._parser_callback = parser_callback

        def register_data_queue(
            self,
            data_queue: weakref.ReferenceType[QueueProtocol],
        ) -> None:
            """Register a new data queue.

            Args:
                data_queue: Weak reference to the data queue to which the values
                    will be added.
            """
            self._data_queues.append(data_queue)

        def _add_to_data_queue(
            self,
            data_queue: QueueProtocol | None,
            value: AnnotatedValue,
        ) -> bool:
            """Add a value to the data queue.

            The value is added to the queue non blocking, meaning that if the queue
            is full, an error is raised.

            Args:
                data_queue: The data queue to which the value will be added.
                value: The value to add to the data queue.

            Returns:
                True if the value was added to the data queue, False otherwise.

            Raises:
                StreamingError: If the data queue is full or disconnected.
                AttributeError: If the data queue has been garbage collected.
            """
            if data_queue is None:
                # The server holds only a weak reference to the data queue.
                # If the data queue has been garbage collected, the weak reference
                # will be None.
                return False
            try:
                data_queue.put_nowait(value)
            except errors.StreamingError:
                logger.debug(
                    "Data queue %s has disconnected. Removing from list of queues.",
                    hex(id(data_queue)),
                )
                return False
            except asyncio.QueueFull:
                logger.warning(
                    "Data queue %s is full. No more data will be pushed to the queue.",
                    hex(id(data_queue)),
                )
                data_queue.disconnect()  # type: ignore[union-attr] # supposed to throw
                return False
            return True

        def _distribute_to_data_queues(
            self,
            value: CapnpCapability,
        ) -> None:
            """Add a value to all data queues.

            The value is added to the queue non blocking, meaning that if the queue
            is full, an error is raised.

            Args:
                value: The value to add to the data queue.

            Raises:
                capnp.KjException: If no data queues are registered any more and
                    the subscription should be removed.
            """
            parsed_value = self._parser_callback(AnnotatedValue.from_capnp(value))
            # distribute to all data queues and remove the ones that are not
            # connected anymore.
            self._data_queues = [
                data_queue
                for data_queue in self._data_queues
                if self._add_to_data_queue(data_queue(), parsed_value)
            ]
            if not self._data_queues:
                # TODO(tobiasa): The kernel expects a KjException of type # noqa: FIX002
                # DISCONNECTED for a clean removal of the subscription. However,
                # pycapnp does currently not support this.
                # https://github.com/capnproto/pycapnp/issues/324
                msg = "DISCONNECTED"
                raise capnp.KjException(
                    type=capnp.KjException.Type.DISCONNECTED,
                    message=msg,
                )

        async def sendValues(  # noqa: N802 (function name is enforced through the schema)
            self,
            values: CapnpCapability,
            **_,
        ) -> None:
            """Capnp Interface callback.

            This function is called by the kernel (through RPC) when an update
            event for the subscribed node is received.

            Args:
                values: List of update events for the subscribed node.

            Raises:
                capnp.KjException: If no data queues are registered any more and
                    the subscription should be removed.
            """
            list(map(self._distribute_to_data_queues, values))

    return StreamingHandleImpl
