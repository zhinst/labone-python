"""Capnp Server method definitions.

This module contains the method definitions for the Capnp Server,
including setting and getting values, listing nodes, and subscribing to
nodes. This specific capnp server methods define the specific
Hpk behavior.
The logic of the capnp methods is delegated to the SessionFunctionality class,
which offers a blueprint meant to be overridden by the user.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import zhinst.comms
from zhinst.comms.server import CapnpResult, CapnpServer, capnp_method

from labone.core import ListNodesFlags, ListNodesInfoFlags, hpk_schema
from labone.core.helper import get_default_context
from labone.core.session import Session
from labone.core.value import (
    AnnotatedValue,
    _capnp_value_to_python_value,
    value_from_python_types,
)

HPK_SCHEMA_ID = 0xA621130A90860008
SESSION_SCHEMA_ID = 0xB9D445582DA4A55C
SERVER_ERROR = "SERVER_ERROR"


if TYPE_CHECKING:
    from labone.core.helper import LabOneNodePath
    from labone.core.session import NodeInfo


class Subscription:
    """Subscription abstraction class.

    This class hides the capnp specific subscription object and the logic to send
    and AnnotatedValue to the subscriber.

    Args:
        path: Path to the node to subscribe to.
        streaming_handle: Capnp specific object to send updates to.
        subscriber_id: Capnp specific id of the subscriber.
    """

    def __init__(
        self,
        path: LabOneNodePath,
        streaming_handle: zhinst.comms.DynamicClient,
        subscriber_id: bytes,
    ):
        self._path = path
        self._streaming_handle = streaming_handle
        self.subscriber_id = subscriber_id

    async def send_value(self, value: AnnotatedValue) -> bool:
        """Send value to the subscriber.

        Args:
            value: Value to send.

        Returns:
            Flag indicating if the subscription is active
        """
        try:
            await self._streaming_handle.sendValues(
                values=[
                    {
                        "value": value_from_python_types(
                            value.value,
                            capability_version=Session.CAPABILITY_VERSION,
                        ),
                        "metadata": {
                            "path": value.path,
                            "timestamp": value.timestamp,
                        },
                    },
                ],
            )
        except zhinst.comms.errors.DisconnectError:
            return False
        return True

    @property
    def path(self) -> LabOneNodePath:
        """Node path of the subscription."""
        return self._path


class MockSession(Session):
    """Regular Session holding a mock server.

    This class is designed for holding the mock server.
    This is needed, because otherwise,
    there would be no reference to the capnp objects, which would go out of scope.
    This way, the correct lifetime of the capnp objects is ensured, by attaching it to
    its client.

    Args:
        mock_server: Mock server.
        capnp_session: Capnp session.
        reflection: Reflection server.
    """

    def __init__(
        self,
        server: LabOneServerBase,
        client: zhinst.comms.DynamicClient,
        *,
        context: zhinst.comms.CapnpContext,
    ):
        super().__init__(
            client,
            context=context,
            capability_version=Session.CAPABILITY_VERSION,
        )
        self._mock_server = server

    @property
    def mock_server(self) -> LabOneServerBase:
        """Mock server."""
        return self._mock_server


def build_capnp_error(error: Exception) -> CapnpResult:
    """Helper function to build a capnp error message.

    Args:
        error: Caught python exception to be converted.

    Returns:
        Capnp Type dictionary for Result(Error).
    """
    return {
        "err": {
            "code": 2,
            "message": f"{error}",
            "category": SERVER_ERROR,
            "source": __name__,
        },
    }


class LabOneServerBase(ABC, CapnpServer):
    """Blueprint for defining the session behavior.

    The SessionFunctionality class offers a interface between
    capnp server logic and the user. The user can override the methods
    to define an individual server. The signature of the methods
    is mostly identical to the session-interface on the caller side.
    Thereby it feels as if the session-interface is overwritten directly,
    hiding the capnp server logic from the user.

    Two possible ways to use this class arise:
     * Call methods indirectly (via capnp server), by having a session
       to a server.
     * Call methods directly. This can be used to manipulate the state
       internally. Limitations of what can be set to a server are
       bypassed. E.g. can be useful when setting SHF vector nodes.

    Both approaches can be combined.
    """

    def __init__(self):
        CapnpServer.__init__(
            self,
            schema=hpk_schema.get_schema_loader().get_interface_schema(HPK_SCHEMA_ID),
        )

    @abstractmethod
    async def get(self, path: LabOneNodePath) -> AnnotatedValue:
        """Override this method for defining get behavior.

        Args:
            path: Path to a single the node.

        Returns:
            Retrieved value.
        """
        ...

    @abstractmethod
    async def get_with_expression(
        self,
        path_expression: LabOneNodePath,
        flags: ListNodesFlags | int = ListNodesFlags.ABSOLUTE
        | ListNodesFlags.RECURSIVE
        | ListNodesFlags.LEAVES_ONLY
        | ListNodesFlags.EXCLUDE_STREAMING
        | ListNodesFlags.GET_ONLY,
    ) -> list[AnnotatedValue]:
        """Override this method for defining get_with_expression behavior.

        Args:
            path_expression: Path expression to get.
            flags: Flags to control the behavior of the get_with_expression method.

        Returns:
            List of values, corresponding to nodes of the path expression.
        """
        ...

    @abstractmethod
    async def set(self, value: AnnotatedValue) -> AnnotatedValue:
        """Override this method for defining set behavior.

        Args:
            value: Value to set. Note that this is in the form of an AnnotatedValue.
                Therefore, value, path and timestamp are encapsulated within.

        Returns:
            Acknowledged value (also in annotated form).
        """
        ...

    @abstractmethod
    async def set_with_expression(self, value: AnnotatedValue) -> list[AnnotatedValue]:
        """Override this method for defining set_with_expression behavior.

        Args:
            value: Value to set. Note that this is in the form of an AnnotatedValue.
                Therefore, value, wildcard-path and timestamp are encapsulated within.
                All nodes matching the wildcard-path will be set to the value.

        Returns:
            Acknowledged values (also in annotated form).
        """
        ...

    @abstractmethod
    async def list_nodes(
        self,
        path: LabOneNodePath = "",
        *,
        flags: ListNodesFlags,
    ) -> list[LabOneNodePath]:
        """Override this method for defining list_nodes behavior.

        Args:
            path: Path to narrow down which nodes should be listed.
                Omitting the path will list all nodes by default.
            flags: Flags to control the behavior of the list_nodes method.

        Returns:
            List of nodes, corresponding to the path and flags.
        """
        ...

    @abstractmethod
    async def list_nodes_info(
        self,
        path: LabOneNodePath,
        *,
        flags: ListNodesInfoFlags,
    ) -> dict[LabOneNodePath, NodeInfo]:
        """Override this method for defining list_nodes_info behavior.

        Args:
            path: Path to narrow down which nodes should be listed.
                Omitting the path will list all nodes by default.
            flags: Flags to control the behavior of the list_nodes_info method.

        Returns:
            Dictionary of paths to node info.
        """
        ...

    @abstractmethod
    async def subscribe(self, subscription: Subscription) -> None:
        """Override this method for defining subscription behavior.

        Args:
            subscription: Subscription object containing information on
                where to distribute updates to.
        """
        ...

    @capnp_method(SESSION_SCHEMA_ID, 7)
    async def _get_session_version_interface(
        self,
        _: hpk_schema.SessionGetSessionVersionParams,
    ) -> CapnpResult:
        """Capnp server method to get session version.

        Returns:
            Capnp result.
        """
        return {"version": str(Session.CAPABILITY_VERSION)}

    @capnp_method(SESSION_SCHEMA_ID, 0)
    async def _list_nodes_interface(
        self,
        call_input: hpk_schema.SessionListNodesParams,
    ) -> CapnpResult:
        """Capnp server method to list nodes.

        Args:
            call_input: Arguments the server has been called with

        Returns:
            Capnp result.
        """
        return {
            "paths": await self.list_nodes(
                call_input.pathExpression,
                flags=ListNodesFlags(call_input.flags),
            ),
        }

    @capnp_method(SESSION_SCHEMA_ID, 5)
    async def _list_nodes_json_interface(
        self,
        call_input: hpk_schema.SessionListNodesJsonParams,
    ) -> CapnpResult:
        """Capnp server method to list nodes json.

        Args:
            call_input: Arguments the server has been called with

        Returns:
            Capnp result.
        """
        return {
            "nodeProps": json.dumps(
                await self.list_nodes_info(
                    call_input.pathExpression,
                    flags=ListNodesInfoFlags(call_input.flags),
                ),
            ),
        }

    @capnp_method(SESSION_SCHEMA_ID, 10)
    async def _get_value_interface(
        self,
        call_input: hpk_schema.SessionGetValueParams,
    ) -> CapnpResult:
        """Capnp server method to get values.

        Args:
            call_input: Arguments the server has been called with

        Returns:
            Capnp result.
        """
        lookup_mode = call_input.lookupMode
        try:
            if lookup_mode == 0:  # direct lookup
                responses = [await self.get(call_input.pathExpression)]
            else:
                responses = await self.get_with_expression(
                    call_input.pathExpression,
                    flags=call_input.flags,
                )
        except Exception as e:  # noqa: BLE001
            return {"result": [build_capnp_error(e)]}

        result = [
            {
                "ok": {
                    "value": value_from_python_types(
                        response.value,
                        capability_version=Session.CAPABILITY_VERSION,
                    ),
                    "metadata": {
                        "path": response.path,
                        "timestamp": response.timestamp,
                    },
                },
            }
            for response in responses
        ]
        return {"result": result}

    @capnp_method(SESSION_SCHEMA_ID, 9)
    async def _set_value_interface(
        self,
        call_input: hpk_schema.SessionSetValueParams,
    ) -> CapnpResult:
        """Capnp server method to set values.

        Args:
            call_input: Arguments the server has been called with

        Returns:
            Capnp result.
        """
        annotated_value = AnnotatedValue(
            value=_capnp_value_to_python_value(call_input.value),
            path=call_input.pathExpression,
        )

        try:
            if call_input.lookupMode == 0:  # direct lookup
                responses = [
                    await self.set(annotated_value),
                ]
            else:
                responses = await self.set_with_expression(
                    annotated_value,
                )
        except Exception as e:  # noqa: BLE001
            return {"result": [build_capnp_error(e)]}

        result = [
            {
                "ok": {
                    "value": value_from_python_types(
                        response.value,
                        capability_version=Session.CAPABILITY_VERSION,
                    ),
                    "metadata": {
                        "path": response.path,
                        "timestamp": response.timestamp,
                    },
                },
            }
            for response in responses
        ]
        return {"result": result}

    @capnp_method(SESSION_SCHEMA_ID, 3)
    async def _subscribe_interface(
        self,
        call_input: hpk_schema.SessionSubscribeParams,
    ) -> CapnpResult:
        """Capnp server method to subscribe to nodes.

        Args:
            call_input: Arguments the server has been called with

        Returns:
            Capnp result.
        """
        try:
            subscription = Subscription(
                path=call_input.subscription.path,
                streaming_handle=call_input.subscription.streamingHandle,
                subscriber_id=call_input.subscription.subscriberId,
            )
            await self.subscribe(subscription)
        except Exception as e:  # noqa: BLE001
            return {"result": [build_capnp_error(e)]}
        return {"result": {"ok": {}}}

    async def start_pipe(  # type: ignore[override]
        self,
        context: zhinst.comms.CapnpContext | None = None,
    ) -> MockSession:
        """Create a local pipe to the server.

        A pipe is a local single connection to the server.

        Args:
            context: context to use.
        """
        if context is None:
            context = get_default_context()
        client = await super().start_pipe(context)
        return MockSession(self, client, context=context)
