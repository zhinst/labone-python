"""Hpk Mock Server method definitions.

This module contains the method definitions for the Hpk Mock Server,
including setting and getting values, listing nodes, and subscribing to
nodes. This specific capnp server methods define the specific
Hpk behavior.
The logic of the capnp methods is deligated to the HpkMockFunctionality class,
which offers a blueprint meant to be overriden by the user.
"""


from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from labone.core import ListNodesFlags, ListNodesInfoFlags
from labone.core.value import (
    AnnotatedValue,
    _capnp_value_to_python_value,
    value_from_python_types_dict,
)
from labone.mock.mock_server import ServerTemplate

if TYPE_CHECKING:
    from capnp.lib.capnp import (
        _CallContext,
        _DynamicEnum,
        _DynamicStructBuilder,
        _DynamicStructReader,
    )


HPK_SCHEMA_ID = 11970870220622790664
SERVER_ERROR = "SERVER_ERROR"


if TYPE_CHECKING:
    from labone.core.helper import LabOneNodePath
    from labone.core.session import NodeInfo
    from labone.core.subscription import StreamingHandle


class SessionMockFunctionality(ABC):
    """Hpk blueprint for defining mock server behavior.

    The HpKMockFunctionality class offers a interface between
    capnp server logic and the user. The user can override the methods
    to define an individual mock server. The signature of the methods
    is mostly identical to the session-interface on the caller side.
    Thereby it feels as if the session-interface is overritten directly,
    hiding the capnp server logic from the user.

    Two possible ways to use this class arise:
     * Call methods indirectly (via capnp server), by having a session
       to a mock server.
     * Call methods directly. This can be used to manipulate the state
       internally. Limitations of what can be set to a server are
       bypassed. E.g. can be useful when setting shf vector nodes.

    Both approaches can be combined.
    """

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
        flags: ListNodesFlags
        | int = ListNodesFlags.ABSOLUTE
        | ListNodesFlags.RECURSIVE
        | ListNodesFlags.LEAVES_ONLY
        | ListNodesFlags.EXCLUDE_STREAMING
        | ListNodesFlags.GET_ONLY,
    ) -> list[AnnotatedValue]:
        """Override this method for defining get_with_expression behavior.

        Args:
            path_expression: Path expression to get.
            flags: Flags to control the behaviour of the get_with_expression method.

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
        flags: ListNodesFlags | int = ListNodesFlags.ABSOLUTE,
    ) -> list[LabOneNodePath]:
        """Override this method for defining list_nodes behavior.

        Args:
            path: Path to narrow down which nodes should be listed.
                Omitting the path will list all nodes by default.
            flags: Flags to control the behaviour of the list_nodes method.

        Returns:
            List of nodes, corresponding to the path and flags.
        """
        ...

    @abstractmethod
    async def list_nodes_info(
        self,
        path: LabOneNodePath = "",
        *,
        flags: ListNodesInfoFlags | int = ListNodesInfoFlags.ALL,
    ) -> dict[LabOneNodePath, NodeInfo]:
        """Override this method for defining list_nodes_info behavior.

        Args:
            path: Path to narrow down which nodes should be listed.
                Omitting the path will list all nodes by default.
            flags: Flags to control the behaviour of the list_nodes_info method.

        Returns:
            Dictionary of paths to node info.
        """
        ...

    @abstractmethod
    async def subscribe_logic(
        self,
        *,
        path: LabOneNodePath,
        streaming_handle: StreamingHandle,
        subscriber_id: int,
    ) -> None:
        """Override this method for defining subscription behavior.

        Args:
            path: Path to the node to subscribe to.
            streaming_handle: Handle to the stream.
            subscriber_id: Unique id of the subscriber.
        """
        ...


def build_capnp_error(error: Exception) -> _DynamicStructBuilder:
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


class SessionMockTemplate(ServerTemplate):
    """Hpk Mock Server.

    The logic for answering capnp requests is outsourced and taken as an argument.
    This allows for custom mock server definition while keeping this classes
    code static.

    Note:
        Methods within serve for capnp to answer requests. They should not be
        called directly. They should not be overritten in order to define
        custom behavior. Instead, override the methods of HpkMockFunctionality.

    Args:
        functionality: The implementation of the mock server behavior.
    """

    # unique capnp id of the Hpk schema
    id_ = HPK_SCHEMA_ID

    def __init__(self, functionality: SessionMockFunctionality) -> None:
        self._functionality = functionality

    async def listNodes(  # noqa: N802
        self,
        pathExpression: str,  # noqa: N803
        flags: ListNodesFlags,
        client: bytes,  # noqa: ARG002
        _context: _CallContext,
        **kwargs,  # noqa: ARG002
    ) -> list[str]:
        """Capnp server method to list nodes.

        Args:
            pathExpression: Path to narrow down which nodes should be listed.
                Omitting the path will list all nodes by default.
            flags: Flags to control the behaviour of the list_nodes method.
            client: Capnp specific argument.
            _context: Capnp specific argument.
            **kwargs: Capnp specific arguments.

        Returns:
            List of paths.
        """
        return await self._functionality.list_nodes(pathExpression, flags=flags)

    async def listNodesJson(  # noqa: N802
        self,
        pathExpression: str,  # noqa: N803
        flags: ListNodesFlags,
        client: bytes,  # noqa: ARG002
        _context: _CallContext,
        **kwargs,  # noqa: ARG002
    ) -> str:
        """Capnp server method to list nodes plus additional informtion as json.

        Args:
            pathExpression: Path to narrow down which nodes should be listed.
                Omitting the path will list all nodes by default.
            flags: Flags to control the behaviour of the list_nodes_info method.
            client: Capnp specific argument.
            _context: Capnp specific argument.
            **kwargs: Capnp specific arguments.

        Returns:
            Json encoded dictionary of paths and node info.
        """
        return json.dumps(
            await self._functionality.list_nodes_info(
                path=pathExpression,
                flags=flags,
            ),
        )

    async def getValue(  # noqa: N802
        self,
        pathExpression: str,  # noqa: N803
        lookupMode: _DynamicEnum,  # noqa: N803
        flags: int,
        client: bytes,  # noqa: ARG002
        _context: _CallContext,
        **kwargs,  # noqa: ARG002
    ) -> list[_DynamicStructBuilder]:
        """Capnp server method to get values.

        Args:
            pathExpression: Path for which the value should be retrieved.
            lookupMode: Defining whether a single path should be retrieved
                or potentially multiple ones specified by a wildcard path.
            flags: Flags to control the behaviour of wildcard path requests.
            client: Capnp specific argument.
            _context: Capnp specific argument.
            **kwargs: Capnp specific arguments.

        Returns:
            List of read values.
        """
        try:
            if lookupMode == 0:  # direct lookup
                responses = [await self._functionality.get(pathExpression)]
            else:
                responses = await self._functionality.get_with_expression(
                    pathExpression,
                    flags=flags,
                )
        except Exception as e:  # noqa: BLE001
            return [build_capnp_error(e)]

        return [
            {
                "ok": {
                    "value": value_from_python_types_dict(response),
                    "metadata": {
                        "path": response.path,
                        "timestamp": response.timestamp,
                    },
                },
            }
            for response in responses
        ]

    async def setValue(  # noqa: PLR0913, N802
        self,
        pathExpression: str,  # noqa: N803
        value: _DynamicStructReader,
        lookupMode: _DynamicEnum,  # noqa: N803
        completeWhen: _DynamicEnum,  # noqa: N803, ARG002
        client: bytes,  # noqa: ARG002
        _context: _CallContext,
        **kwargs,  # noqa: ARG002
    ) -> list[_DynamicStructBuilder]:
        """Capnp server method to set values.

        Args:
            pathExpression: Path for which the value should be set.
            value: Value to be set.
            lookupMode: Defining whether a single path should be set
                or potentially multiple ones specified by a wildcard path.
            completeWhen: Capnp specific argument.
            client: Capnp specific argument.
            _context: Capnp specific argument.
            **kwargs: Capnp specific arguments.

        Returns:
            List of acknowledged values.
        """
        value, extra_header = _capnp_value_to_python_value(value)
        annotated_value = AnnotatedValue(
            value=value,
            path=pathExpression,
            extra_header=extra_header,
        )

        try:
            if lookupMode == 0:  # direct lookup
                responses = [
                    await self._functionality.set(annotated_value),
                ]
            else:
                responses = await self._functionality.set_with_expression(
                    annotated_value,
                )
        except Exception as e:  # noqa: BLE001
            return [build_capnp_error(e)]

        return [
            {
                "ok": {
                    "value": value_from_python_types_dict(response),
                    "metadata": {
                        "path": response.path,
                        "timestamp": response.timestamp,
                    },
                },
            }
            for response in responses
        ]

    async def subscribe(
        self,
        subscription: _DynamicStructReader,
        _context: _CallContext,
        **kwargs,  # noqa: ARG002
    ) -> _DynamicStructBuilder:
        """Capnp server method to subscribe to nodes.

        Do not override this method. Instead, override 'subscribe_logic'
        of HpkMockFunctionality (or subclass).

        Args:
            subscription: Capnp object containing information on
                where to distribute updates to.
            _context: Capnp specific argument.
            **kwargs: Capnp specific arguments.

        Returns:
            Capnp acknowledgement.
        """
        try:
            await self._functionality.subscribe_logic(
                path=subscription.path,
                streaming_handle=subscription.streamingHandle,
                subscriber_id=subscription.subscriberId,
            )
        except Exception as e:  # noqa: BLE001
            return build_capnp_error(e)
        return {"ok": {}}
