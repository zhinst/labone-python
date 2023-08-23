"""Utility module for `labone.core` tests."""
import asyncio
import socket
from functools import wraps

import capnp


def ensure_event_loop(f):
    """Ensures that the wrapped coroutine is completed.

    This is a workaround for making `capnp` event loops to work with
    `pytest`.
    """

    @wraps(f)
    def inner(*args, **kwargs):
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(f(*args, **kwargs))
        loop.run_until_complete(future)

    return inner


class CapnpServer:
    """A capnp server."""

    def __init__(self, connection: capnp.AsyncIoStream):
        self._connection = connection

    @property
    def connection(self) -> capnp.AsyncIoStream:
        """Connection to the server."""
        return self._connection

    @classmethod
    async def create(
        cls,
        obj: capnp.lib.capnp._DynamicCapabilityServer,
    ) -> "CapnpServer":
        """Create a server for the given object."""
        read, write = socket.socketpair()
        write = await capnp.AsyncIoStream.create_connection(sock=write)
        _ = asyncio.create_task(cls._new_connection(write, obj))
        return cls(await capnp.AsyncIoStream.create_connection(sock=read))

    @staticmethod
    async def _new_connection(
        stream: capnp.AsyncIoStream,
        obj: capnp.lib.capnp._DynamicCapabilityServer,
    ):
        """Establish a new connection."""
        await capnp.TwoPartyServer(stream, bootstrap=obj).on_disconnect()
