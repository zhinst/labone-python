"""Module for a session to a Zurich Instruments Session Capability.

The Session capability provides access to the basic interactions with nodes
and or properties of a server. Its used in multiple places with the software
stack of Zurich Instruments. For example the Data Server kernel for a device
provides a Session capability to access the nodes of the device.

Every Session capability provides the same capnp interface and can therefore be
handled in the same way. The only difference is the set of nodes/properties that
are available.

The number of sessions to a capability is not limited. However, due to the
asynchronous interface, it is often not necessary to have multiple sessions
to the same capability.
"""
from __future__ import annotations

import json
import typing as t
import uuid
from enum import IntFlag
from typing import Literal

import capnp
from typing_extensions import NotRequired, TypeAlias, TypedDict

from labone.core import errors, result
from labone.core.helper import (
    CapnpCapability,
    LabOneNodePath,
    request_field_type_description,
)
from labone.core.result import unwrap
from labone.core.subscription import DataQueue, QueueProtocol, streaming_handle_factory
from labone.core.value import AnnotatedValue

if t.TYPE_CHECKING:
    from labone.core.reflection.server import ReflectionServer

T = t.TypeVar("T")

NodeType: TypeAlias = Literal[
    "Integer (64 bit)",
    "Double",
    "Complex Double",
    "String",
    "ZIScopeWave",
    "ZIImpedanceSample",
    "ZICntSample",
    "ZITrigSample",
    "ZIVectorData",
    "ZIDemodSample",
    "ZIPWAWave",
    "ZIAuxInSample",
    "ZIDIOSample",
]


class NodeInfo(TypedDict):
    """Node information structure.

    Args:
        Node: Node absolute path.
        Description: Node description.
        Properties: Comma-separated list of node properties.
            A node can have one or multiple of the following properties:

                - "Read", "Write", "Stream", "Setting", "Pipelined"

            Example: "Read, Write"

        Type: Node type.
        Unit: Node unit.
        Options: Possible values for the node.
            The key exists only if the node `Type` is `Integer (enumerated)`.
    """

    Node: LabOneNodePath
    Description: str
    Properties: str
    Type: NodeType
    Unit: str
    Options: NotRequired[dict[str, str]]


class ListNodesInfoFlags(IntFlag):
    """Options for specifying on how `Session.list_nodes_info()` returns the nodes.

    Multiple flags can be combined by using bitwise operations:

    >>> ListNodesInfoFlags.SETTINGS_ONLY | ListNodesInfoFlags.EXCLUDE_VECTORS

    Args:
        ALL: Return all matching nodes.
        SETTINGS_ONLY: Return only setting nodes.
        STREAMING_ONLY: Return only streaming nodes.
        BASE_CHANNEL_ONLY: Return only one instance of a channel
            in case of multiple channels.
        GET_ONLY: Return only nodes which can be used with the get command.
        EXCLUDE_STREAMING: Exclude streaming nodes.
        EXCLUDE_VECTORS: Exclude vector nodes.
    """

    ALL = 0
    SETTINGS_ONLY = 1 << 3
    STREAMING_ONLY = 1 << 4
    BASE_CHANNEL_ONLY = 1 << 6
    GET_ONLY = 1 << 7
    EXCLUDE_STREAMING = 1 << 20
    EXCLUDE_VECTORS = 1 << 24


class ListNodesFlags(IntFlag):
    """Options for specifying on how `Session.list_nodes()` returns the nodes.

    Multiple flags can be combined by using bitwise operations:

    >>> ListNodesFlags.ABSOLUTE | ListNodesFlags.RECURSIVE

    Args:
        ALL: Return all matching nodes.
        RECURSIVE: Return nodes recursively.
        ABSOLUTE: Absolute node paths.
        LEAVES_ONLY: Return only leave nodes, which means they
            are at the outermost level of the three.
        SETTINGS_ONLY: Return only setting nodes.
        STREAMING_ONLY: Return only streaming nodes.
        BASE_CHANNEL_ONLY: Return only one instance of a channel
            in case of multiple channels.
        GET_ONLY: Return only nodes which can be used with the get command.
        EXCLUDE_STREAMING: Exclude streaming nodes.
        EXCLUDE_VECTORS: Exclude vector nodes.
    """

    ALL = 0
    RECURSIVE = 1
    ABSOLUTE = 1 << 1
    LEAVES_ONLY = 1 << 2
    SETTINGS_ONLY = 1 << 3
    STREAMING_ONLY = 1 << 4
    BASE_CHANNEL_ONLY = 1 << 6
    GET_ONLY = 1 << 7
    EXCLUDE_STREAMING = 1 << 20
    EXCLUDE_VECTORS = 1 << 24


async def _send_and_wait_request(
    request: capnp.lib.capnp._Request,  # noqa: SLF001
) -> capnp.lib.capnp._Response:  # noqa: SLF001
    """Send a request and wait for the response.

    The main purpose of this function is to take care of the error handling
    for the capnp communication. If an error occurs a LabOneCoreError is
    raised.

    Args:
        request: Capnp request.

    Returns:
        Successful response.

    Raises:
        LabOneCoreError: If sending the message or receiving the response failed.
        UnavailableError: If the server has not implemented the requested
            method.
    """
    try:
        return await request.send()
    # TODO(markush): Raise more specific error types.  # noqa: TD003, FIX002
    except capnp.lib.capnp.KjException as error:
        if (
            "Method not implemented" in error.description
            or "object has no attribute" in error.description
        ):
            msg = str(
                "The requested method is not implemented by the server. "
                "This most likely that the LabOne version is outdated. "
                "Please update the LabOne software to the latest version.",
            )
            raise errors.UnavailableError(msg) from None
        msg = error.description
        raise errors.LabOneCoreError(msg) from None
    except Exception as error:  # noqa: BLE001
        msg = str(error)
        raise errors.LabOneCoreError(msg) from None


class Session:
    """Generic Capnp session client.

    Representation of a single Session capability. This class
    encapsulates the capnp interaction an exposes a python native api.
    All function are exposed as they are implemented in the interface
    of the capnp server and are directly forwarded.

    Each function implements the required error handling both for the
    capnp communication and the server errors. This means unless an Exception
    is raised the call was successful.

    The Session already requires an existing connection this is due to the
    fact that the instantiation is done asynchronously. In addition the underlying
    reflection server is required to be able to create the capnp messages.

    Args:
        capnp_session: Capnp session capability.
        reflection_server: Reflection server instance.
    """

    def __init__(
        self,
        capnp_session: CapnpCapability,
        *,
        reflection_server: ReflectionServer,
    ):
        self._reflection_server = reflection_server
        self._session = capnp_session
        # The client_id is required by most capnp messages to identify the client
        # on the server side. It is unique per session.
        self._client_id = uuid.uuid4()

    async def list_nodes(
        self,
        path: LabOneNodePath = "",
        *,
        flags: ListNodesFlags | int = ListNodesFlags.ABSOLUTE,
    ) -> list[LabOneNodePath]:
        """List the nodes found at a given path.

        Args:
            path: A string representing the path where the nodes are to be listed.
                Value is case insensitive.

                Supports asterix (*) wildcards, except in the place of node path forward
                slashes. (default="")
            flags: The flags for modifying the returned nodes.

        Returns:
            A list of strings representing the nodes.

            Returns an empty list when `path` does not match any nodes or the nodes
                matching `path` does not fit into given `flags` criteria.

        Raises:
            TypeError: If `path` is not a string or `flags` is not an integer.
            ValueError: If `flags` value is out-of-bounds.
            OverwhelmedError: If the kernel is overwhelmed.
            UnimplementedError: If the list nodes request is not
                supported by the server.
            InternalError: If an unexpected internal error occurs.
            LabOneCoreError: If something else went wrong that can not be
                mapped to one of the other errors.


        Examples:
            Getting all the nodes:

            >>> await session.list_nodes("*")
            ["/zi/config", "/zi/about", "/zi/debug", "/zi/clockbase", \
                "/zi/devices", "/zi/mds"]

            Getting the nodes with a specific path:

            >>> await session.list_nodes("zi/devices")
            ["/zi/visible", "/zi/connected"]

            Using wildcards in the place of forward slashes will result
            to an empty list:

            >>> await session.list_nodes("zi*devices")
            []

            Using flags:

            >>> await session.list_nodes("zi/devices", flags=ListNodesFlags.RECURSIVE \
                | ListNodesFlags.EXCLUDE_VECTORS)
            ...
        """
        request = self._session.listNodes_request()
        try:
            request.pathExpression = path
        except Exception:  # noqa: BLE001
            msg = "`path` must be a string."
            raise TypeError(msg)  # noqa: TRY200, B904
        try:
            request.flags = int(flags)
        except capnp.KjException:
            field_type = request_field_type_description(request, "flags")
            msg = f"`flags` value is out-of-bounds, it must be of type {field_type}."
            raise ValueError(
                msg,
            ) from None
        except (TypeError, ValueError):
            msg = "`flags` must be an integer."
            raise TypeError(msg) from None
        response = await _send_and_wait_request(request)
        return list(response.paths)

    async def list_nodes_info(
        self,
        path: LabOneNodePath = "",
        *,
        flags: ListNodesInfoFlags | int = ListNodesInfoFlags.ALL,
    ) -> dict[LabOneNodePath, NodeInfo]:
        """List the nodes and their information found at a given path.

        Args:
            path: A string representing the path where the nodes are to be listed.
                Value is case insensitive.

                Supports asterix (*) wildcards, except in the place of node path
                forward slashes. (default="")

            flags: The flags for modifying the returned nodes.

        Returns:
            A python dictionary where absolute node paths are keys and their
                information are values.

            An empty dictionary when `path` does not match any nodes or the nodes
                matching `path` does not fit into given `flags` criteria.

        Raises:
            TypeError: If `path` is not a string or `flags` is not an integer.
            ValueError: If `flags` value is out-of-bounds.
            OverwhelmedError: If the kernel is overwhelmed.
            UnimplementedError: If the list nodes info request is not
                supported by the server.
            InternalError: If an unexpected internal error occurs.
            LabOneCoreError: If something else went wrong that can not be
                mapped to one of the other errors.

        Example:
            Using a wildcard in the node path that matches multiple nodes:

            >>> await session.list_nodes_info("/zi/devices/*")
            {
                '/zi/devices/visible': {
                    'Node': '/ZI/DEVICES/VISIBLE',
                    'Description': 'Contains a list of devices in the network' \
                        'visible to the LabOne Data Server.',
                    'Properties': 'Read',
                    'Type': 'String',
                    'Unit': 'None'
                },
                '/zi/devices/connected': {
                    'Node': '/ZI/DEVICES/CONNECTED',
                    'Description': 'Contains a list of devices connected to the' \
                        'LabOne Data Server.',
                    'Properties': 'Read',
                    'Type': 'String',
                    'Unit': 'None'
                }
            }

            With nodes of type 'Integer (enumerated)', the returned
            value has an additional 'Options" key:

            >>> await session.list_nodes_info("/zi/config/open")
            {
                '/zi/config/open': {
                    'Node': '/ZI/CONFIG/OPEN',
                    'Description': 'Enable communication with the LabOne Data Server' \
                        'from other computers in the network.',
                    'Properties': 'Read, Write, Setting',
                    'Type': 'Integer (enumerated)',
                    'Unit': 'None',
                    'Options': {
                        '0': '"local": Communication only possible with ' \
                            'the local machine.',
                        '1': '"network": Communication possible with other' \
                            'machines in the network.'
                    }
                }
            }
        """
        request = self._session.listNodesJson_request()
        try:
            request.pathExpression = path
        except Exception:  # noqa: BLE001
            msg = "`path` must be a string."
            raise TypeError(msg) from None
        try:
            request.flags = int(flags)
        except capnp.KjException:
            field_type = request_field_type_description(request, "flags")
            msg = f"`flags` value is out-of-bounds, it must be of type {field_type}."
            raise ValueError(
                msg,
            ) from None
        except (TypeError, ValueError):
            msg = "`flags` must be an integer."
            raise TypeError(msg) from None
        response = await _send_and_wait_request(request)
        return json.loads(response.nodeProps)

    async def set(self, value: AnnotatedValue) -> AnnotatedValue:  # noqa: A003
        """Set the value of a node.

        Args:
            value: Value to be set. The annotated value must contain a
                LabOne node path and a value. (The path can be relative or
                absolute.)

        Returns:
            Acknowledged value from the device.

        Example:
            >>> await session.set(AnnotatedValue(path="/zi/debug/level", value=2)

        Raises:
            TypeError: If the node path is of wrong type.
            NotFoundError: If the node path does not exist.
            OverwhelmedError: If the kernel is overwhelmed.
            BadRequestError: If the path is not settable.
            UnimplementedError: If the set request is not
                supported by the server.
            InternalError: If an unexpected internal error occurs.
            LabOneCoreError: If something else went wrong that can not be
                mapped to one of the other errors.
        """
        capnp_value = value.to_capnp(reflection=self._reflection_server)
        request = self._session.setValue_request()
        request.pathExpression = capnp_value.metadata.path
        request.value = capnp_value.value
        request.lookupMode = (
            self._reflection_server.LookupMode.directLookup  # type: ignore[attr-defined]
        )
        request.client = self._client_id.bytes
        response = await _send_and_wait_request(request)
        try:
            return AnnotatedValue.from_capnp(result.unwrap(response.result[0]))
        except IndexError as e:
            msg = f"No acknowledgement returned while setting {value.path}."
            raise errors.LabOneCoreError(msg) from e

    async def set_with_expression(self, value: AnnotatedValue) -> list[AnnotatedValue]:
        """Set the value of all nodes matching the path expression.

        A path expression is a labone node path. The difference to a normal
        node path is that it can contain wildcards and must not be a leaf node.
        In short it is a path that can match multiple nodes. For more information
        on path expressions see the `list_nodes()` function.

        If an error occurs while fetching the values no value is returned but
        the first first exception instead.

        Args:
            value: Value to be set. The annotated value must contain a
                LabOne path expression and a value.

        Returns:
            Acknowledged value from the device.

        Example:
            >>> ack_values = await session.set_with_expression(
                    AnnotatedValue(path="/zi/*/level", value=2)
                )
            >>> print(ack_values[0])

        Raises:
            TypeError: If the node path is of wrong type.
            NotFoundError: If the node path does not exist.
            OverwhelmedError: If the kernel is overwhelmed.
            BadRequestError: If the path is not settable.
            UnimplementedError: If the set with expression request is not
                supported by the server.
            InternalError: If an unexpected internal error occurs.
            LabOneCoreError: If something else went wrong that can not be
                mapped to one of the other errors.
        """
        capnp_value = value.to_capnp(reflection=self._reflection_server)
        request = self._session.setValue_request()
        request.pathExpression = capnp_value.metadata.path
        request.value = capnp_value.value
        request.lookupMode = self._reflection_server.LookupMode.withExpansion  # type: ignore[attr-defined]
        request.client = self._client_id.bytes
        response = await _send_and_wait_request(request)
        return [
            AnnotatedValue.from_capnp(result.unwrap(raw_result))
            for raw_result in response.result
        ]

    async def get(
        self,
        path: LabOneNodePath,
    ) -> AnnotatedValue:
        """Get the value of a node.

         The node can either be passed as an absolute path, starting with a leading
         slash and the device id (e.g. "/dev123/demods/0/enable") or as relative
         path (e.g. "demods/0/enable"). In the latter case the device id is
         automatically added to the path by the server. Note that the
         orchestrator/ZI kernel always requires absolute paths (/zi/about/version).

        Args:
            path: LabOne node path (relative or absolute).

        Returns:
            Annotated value of the node.

        Example:
            >>> await session.get('/zi/devices/visible')

        Raises:
            TypeError: If `path` is not a string.
            NotFoundError: If the path does not exist.
            OverwhelmedError: If the kernel is overwhelmed.
            BadRequestError: If the path is not readable.
            UnimplementedError: If the get request is not supported
                by the server.
            InternalError: If an unexpected internal error occurs.
            LabOneCoreError: If something else went wrong that can not be
                mapped to one of the other errors.
        """
        request = self._session.getValue_request()
        try:
            request.pathExpression = path
        except (AttributeError, TypeError, capnp.KjException) as error:
            field_type = request_field_type_description(request, "pathExpression")
            msg = f"`path` attribute must be of type {field_type}."
            raise TypeError(msg) from error
        request.lookupMode = self._reflection_server.LookupMode.directLookup  # type: ignore[attr-defined]
        request.client = self._client_id.bytes
        response = await _send_and_wait_request(request)
        try:
            return AnnotatedValue.from_capnp(result.unwrap(response.result[0]))
        except IndexError as e:
            msg = f"No value returned for {path}."
            raise errors.LabOneCoreError(msg) from e

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
        """Get the value of all nodes matching the path expression.

        A path expression is a labone node path. The difference to a normal
        node path is that it can contain wildcards and must not be a leaf node.
        In short it is a path that can match multiple nodes. For more information
        on path expressions see the `list_nodes()` function.

        If an error occurs while fetching the values no value is returned but
        the first first exception instead.

        Args:
            path_expression: LabOne path expression.
            flags: The flags used by the server (list_nodes()) to filter the
                nodes.

        Returns:
            Annotated values from the nodes matching the path expression.

        Example:
            >>> values = await session.get_with_expression("/zi/*/level")
            >>> print(values[0])

        Raises:
            TypeError: If the node path is of wrong type.
            OverwhelmedError: If the kernel is overwhelmed.
            BadRequestError: If the path is not readable.
            UnimplementedError: If the get with expression request is not
                supported by the server.
            InternalError: If an unexpected internal error occurs
            LabOneCoreError: If something else went wrong that can not be
                mapped to one of the other errors.
        """
        request = self._session.getValue_request()
        try:
            request.pathExpression = path_expression
        except (AttributeError, TypeError, capnp.KjException) as error:
            field_type = request_field_type_description(request, "pathExpression")
            msg = f"`path` attribute must be of type {field_type}."
            raise TypeError(msg) from error
        request.lookupMode = self._reflection_server.LookupMode.withExpansion  # type: ignore[attr-defined]
        request.flags = int(flags)
        request.client = self._client_id.bytes
        response = await _send_and_wait_request(request)
        return [
            AnnotatedValue.from_capnp(result.unwrap(raw_result))
            for raw_result in response.result
        ]

    @t.overload
    async def subscribe(
        self,
        path: LabOneNodePath,
        *,
        parser_callback: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
        queue_type: None = None,
        get_initial_value: bool,
    ) -> DataQueue:
        ...

    @t.overload
    async def subscribe(
        self,
        path: LabOneNodePath,
        *,
        parser_callback: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
        queue_type: type[QueueProtocol],
        get_initial_value: bool,
    ) -> QueueProtocol:
        ...

    async def subscribe(
        self,
        path: LabOneNodePath,
        *,
        parser_callback: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
        queue_type: type[QueueProtocol] | None = None,
        get_initial_value: bool = False,
    ) -> QueueProtocol | DataQueue:
        """Register a new subscription to a node.

        Registers a new subscription to a node on the kernel/server. All
        updates to the node will be pushed to the returned data queue.

        Note:
            An update is triggered by the device itself and does not
            exclusively mean a change in the value of the node. For example
            a set request from any client will also trigger an update event.

        It is safe to have multiple subscriptions to the same path. However
        in most cases it is more efficient to fork (DataQueue.fork) an
        existing DataQueue rather then registering a new subscription at the
        server. This is because the kernel/server will send the update events
        to every registered subscription independently, causing additional
        network overhead.

        Args:
            path: String representing the path of the node to be streamed.
                Currently does not support wildcards in the path.
            parser_callback: Function to bring values obtained from
                data-queue into desired format. This may involve parsing
                them or putting them into an enum.
            queue_type: The type of the queue to be returned. This can be
                any class matching the DataQueue interface. Only needed if the
                default DataQueue class is not sufficient. If None is passed
                the default DataQueue class is used. (default=None)
            get_initial_value: If True, the initial value of the node is
                is placed in the queue. (default=False)

        Returns:
            An instance of the DataQueue class. This async queue will receive
            all update events for the subscribed node.

        Example:
            >>> data_sink = await session.subscribe("/zi/devices/visible")
            >>> newly_detected_device = await data_sink.get()

        Raises:
            TypeError: If `path` is not a string
            NotFoundError: If the path does not exist.
            OverwhelmedError: If the kernel is overwhelmed.
            BadRequestError: If the path can not be subscribed.
            UnimplementedError: If the subscribe request is not supported
                by the server.
            InternalError: If an unexpected internal error occurs.
            LabOneCoreError: If something else went wrong that can not be
                mapped to one of the other errors.
        """
        streaming_handle = streaming_handle_factory(self._reflection_server)(
            parser_callback=parser_callback,
        )
        subscription = self._reflection_server.Subscription(  # type: ignore[attr-defined]
            streamingHandle=streaming_handle,
            subscriberId=self._client_id.bytes,
        )
        try:
            subscription.path = path
        except (AttributeError, TypeError, capnp.KjException):
            field_type = request_field_type_description(subscription, "path")
            msg = f"`path` attribute must be of type {field_type}."
            raise TypeError(msg) from None
        request = self._session.subscribe_request()
        request.subscription = subscription

        if get_initial_value:
            response, initial_value = await asyncio.gather(
                _send_and_wait_request(request),
                self.get(path),
            )
        else:
            response = await _send_and_wait_request(request)
            initial_value = None

        unwrap(response.result)  # Result(Void, Error)
        new_queue_type = queue_type or DataQueue
        queue = new_queue_type(
            path=path,
            register_function=streaming_handle.register_data_queue,
        )
        if initial_value is not None:
            queue.put_nowait(initial_value)
        return queue


    @property
    def reflection_server(self) -> ReflectionServer:
        """Get the reflection server instance."""
        return self._reflection_server  # pragma: no cover
