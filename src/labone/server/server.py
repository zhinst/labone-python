"""Abstract reflection as base for servers.

A capnp reflection server is dynamically created from a binary schema file.
This server can provide the schema (getTheSchema) and takes
an additional server used for the actual functionality. This functionality
inserting allows for a abstract common reflection server.
"""

from __future__ import annotations

import typing as t

import zhinst.comms
from typing_extensions import TypeAlias

from labone.core.helper import get_default_context

CapnpResult: TypeAlias = dict[str, t.Any]


def capnp_method(interface: int, method_index: int) -> t.Callable:
    """A decorator indicate that a function is capnp callback."""

    def inner(func: t.Callable) -> t.Callable:
        func._capnp_info = (  # type:ignore[attr-defined]  # noqa: SLF001
            interface,
            method_index,
        )
        return func

    return inner


class CapnpServer:
    """Basic capnp server.

    Todo ...

    Args:
        schema: schema to use.
    """

    def __init__(self, schema: zhinst.comms.SchemaLoader):
        self._schema = schema
        self._registered_callbacks: dict[tuple[int, int], t.Callable] = {}
        self._load_callbacks()

    def _load_callbacks(self) -> None:
        """Load all methods with the capnp_method decorator."""
        for method_name in dir(self):
            method = getattr(self, method_name)
            capnp_info = getattr(method, "_capnp_info", None)
            if capnp_info:
                self._registered_callbacks[capnp_info] = method

    async def _capnp_callback(
        self,
        interface: int,
        method_index: int,
        call_input: zhinst.comms.DynamicStructBase,
        fulfiller: zhinst.comms.Fulfiller,
    ) -> None:
        """Entrypoint for all capnp calls.

        This method called by capnp whenever a new request is received.

        Args:
            interface: Interface of the call.
            method_index: Method index of the call.
            call_input: Input of the call.
            fulfiller: Fulfiller to fulfill or reject the call.
        """
        target_info = (interface, method_index)
        if target_info not in self._registered_callbacks:
            fulfiller.reject(
                zhinst.comms.Fulfiller.UNIMPLEMENTED,
                f"Function {interface}:{method_index} not implemented",
            )
            return
        try:
            fulfiller.fulfill(await self._registered_callbacks[target_info](call_input))
        except Exception as e:  # noqa: BLE001
            fulfiller.reject(zhinst.comms.Fulfiller.DISCONNECTED, str(e.args[0]))

    async def start(
        self,
        port: int,
        *,
        open_overwrite: bool = False,
        context: zhinst.comms.CapnpContext,
    ) -> None:
        """Start the server on a given port.

        Args:
            context: context to use.
            port: port to listen on.
            open_overwrite: Flag if the server should be reachable from outside.
        """
        self._capnp_server = await context.listen(
            port=port,
            openOverride=open_overwrite,
            callback=self._capnp_callback,
            schema=self._schema,
        )

    async def start_pipe(
        self,
        context: zhinst.comms.CapnpContext | None = None,
    ) -> zhinst.comms.DynamicClient:
        """Create a local pipe to the server.

        A pipe is a local single connection to the server.

        Args:
            context: context to use.
        """
        if context is None:
            context = get_default_context()
        self._capnp_server, client = await context.create_pipe(
            server_callback=self._capnp_callback,
            schema=self._schema,
        )
        return client
