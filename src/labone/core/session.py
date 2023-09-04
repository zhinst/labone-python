"""Module for a session to a LabOne Kernel.

A Kernel is a remote server that provides access to a defined set of nodes.
It can be a device kernel that provides access to the device nodes but it
can also be a kernel that provides additional functionality, e.g. the
Data Server (zi) kernel.

Every Kernel provides the same capnp interface and can therefore be handled
in the same way. The only difference is the set of nodes that are available
on the kernel.

The number of sessions to a kernel is not limited. However, due to the
asynchronous interface, it is often not necessary to have multiple sessions
to the same kernel.
"""
from __future__ import annotations

import json
import uuid
from enum import IntFlag

import capnp
from typing_extensions import Literal, NotRequired, TypeAlias, TypedDict

from labone.core import errors, result
from labone.core.connection_layer import (
    KernelInfo,
    ServerInfo,
    create_session_client_stream,
)
from labone.core.helper import request_field_type_description
from labone.core.resources import session_protocol_capnp  # type: ignore[attr-defined]
from labone.core.result import unwrap
from labone.core.subscription import DataQueue, StreamingHandle
from labone.core.value import AnnotatedValue

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

    Node: str
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

    Args:
        request: Capnp request.

    Returns:
        Successful response.

    Raises:
        LabOneConnectionError: If sending the message or receiving the response failed.
    """
    try:
        return await request.send().a_wait()
    # TODO(markush): Raise more specific error types.  # noqa: TD003, FIX002
    except capnp.lib.capnp.KjException as error:
        msg = error.description
        raise errors.LabOneConnectionError(msg) from error
    except Exception as error:  # noqa: BLE001
        msg = str(error)
        raise errors.LabOneConnectionError(msg) from error


class KernelSession:
    """Capnp session client.

    TODO document

    Args:
        connection: Asyncio stream connection to the server.
    """

    def __init__(
        self,
        connection: capnp.AsyncIoStream,
        kernel_info: KernelInfo,
        server_info: ServerInfo,
    ) -> None:
        self._client = capnp.TwoPartyClient(connection)
        self._kernel_info = kernel_info
        self._server_info = server_info
        self._session = self._client.bootstrap().cast_as(session_protocol_capnp.Session)
        # The client_id is required by most capnp messages to identify the client
        # on the server side. It is unique per session.
        self._client_id = uuid.uuid4()

    @staticmethod
    async def create(
        *,
        kernel_info: KernelInfo,
        server_info: ServerInfo,
    ) -> KernelSession:
        """Create a new session to a LabOne kernel.

        Since the creation of a new session happens asynchronously, this method
        is required, instead of a simple constructor (since a constructor can
        not be async).

        Warning: The initial socket creation and setup (handshake, ...) is
            currently not done asynchronously! The reason is that there is not
            easy way of doing this with the current capnp implementation.

        Args:
            kernel_info: Information about the target kernel.
            server_info: Information about the target data server.

        Returns:
            A new session to the specified kernel.

        Raises:
            KernelNotFoundError: If the kernel was not found.
            IllegalDeviceIdentifierError: If the device identifier is invalid.
            DeviceNotFoundError: If the device was not found.
            KernelLaunchFailureError: If the kernel could not be launched.
            FirmwareUpdateRequiredError: If the firmware of the device is outdated.
            InterfaceMismatchError: If the interface does not match the device.
            DifferentInterfaceInUseError: If the device is visible, but cannot be
                connected through the requested interface.
            DeviceInUseError: If the device is already in use.
            BadRequestError: If there is a generic problem interpreting the incoming
                request.
            LabOneConnectionError: If another error happens during the session creation.
        """
        sock, kernel_info_extended, server_info_extended = create_session_client_stream(
            kernel_info=kernel_info,
            server_info=server_info,
        )
        connection = await capnp.AsyncIoStream.create_connection(sock=sock)
        return KernelSession(
            connection=connection,
            kernel_info=kernel_info_extended,
            server_info=server_info_extended,
        )

    @property
    def kernel_info(self) -> KernelInfo:
        """Information about the kernel."""
        return self._kernel_info

    @property
    def server_info(self) -> ServerInfo:
        """Information about the server."""
        return self._server_info

    async def list_nodes(
        self,
        path: str,
        flags: ListNodesFlags | int = ListNodesFlags.ABSOLUTE,
    ) -> list[str]:
        """List the nodes found at a given path.

        Args:
            path: A string representing the path where the nodes are to be listed.
                Value is case insensitive.

                Supports asterix (*) wildcards, except in the place of node path forward
                slashes.
            flags: The flags for modifying the returned nodes.

        Returns:
            A list of strings representing the nodes.

            Returns an empty list when `path` does not match any nodes or the nodes
                matching `path` does not fit into given `flags` criteria.

        Raises:
            TypeError: If `path` is not a string or `flags` is not an integer.
            ValueError: If `flags` value is out-of-bounds.
            LabOneConnectionError: If there is a problem in the connection.

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
        except capnp.KjException as error:
            field_type = request_field_type_description(request, "flags")
            msg = f"`flags` value is out-of-bounds, it must be of type {field_type}."
            raise ValueError(
                msg,
            ) from error
        except (TypeError, ValueError) as error:
            msg = "`flags` must be an integer."
            raise TypeError(msg) from error
        response = await _send_and_wait_request(request)
        return list(response.paths)

    async def list_nodes_info(
        self,
        path: str,
        flags: ListNodesInfoFlags | int = ListNodesInfoFlags.ALL,
    ) -> dict[str, NodeInfo]:
        """List the nodes and their information found at a given path.

        Args:
            path: A string representing the path where the nodes are to be listed.
                Value is case insensitive.

                Supports asterix (*) wildcards, except in the place of node path
                forward slashes.

            flags: The flags for modifying the returned nodes.

        Returns:
            A python dictionary where absolute node paths are keys and their
                information are values.

            An empty dictionary when `path` does not match any nodes or the nodes
                matching `path` does not fit into given `flags` criteria.

        Raises:
            TypeError: If `path` is not a string or `flags` is not an integer.
            ValueError: If `flags` value is out-of-bounds.
            LabOneConnectionError: If there is a problem in the connection.

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
            raise TypeError(msg)  # noqa: TRY200, B904
        try:
            request.flags = int(flags)
        except capnp.KjException as error:
            field_type = request_field_type_description(request, "flags")
            msg = f"`flags` value is out-of-bounds, it must be of type {field_type}."
            raise ValueError(
                msg,
            ) from error
        except (TypeError, ValueError) as error:
            msg = "`flags` must be an integer."
            raise TypeError(msg) from error
        response = await _send_and_wait_request(request)
        return json.loads(response.nodeProps)

    async def set_value(self, value: AnnotatedValue) -> AnnotatedValue:
        """Set the value of a node.

        Args:
            value: Annotated value of the node.

        Returns:
            Acknowledged value from the device.

        Raises:
            TypeError: If the node path is of wrong type.
            LabOneCoreError: If the node value type is not supported.
            LabOneConnectionError: If there is a problem in the connection.
        """
        # T O D O: To accept list of `AnnotatedValue`s
        capnp_value = value.to_capnp()
        request = self._session.setValue_request()
        request.path = capnp_value.metadata.path
        request.value = capnp_value.value
        response = await _send_and_wait_request(request)
        return AnnotatedValue.from_capnp(result.unwrap(response.result))

    async def subscribe(self, path: str) -> DataQueue:
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

        Returns:
            An instance of the DataQueue class. This async queue will receive
            all update events for the subscribed node.

        Raises:
            TypeError: If `path` is not a string
            LabOneConnectionError: If there is a problem in the connection.
        """
        streaming_handle = StreamingHandle()
        subscription = session_protocol_capnp.Subscription(
            streamingHandle=streaming_handle,
            subscriberId=self._client_id.bytes,
        )
        try:
            subscription.path = path
        except (AttributeError, TypeError, capnp.KjException) as error:
            field_type = request_field_type_description(subscription, "path")
            msg = f"`path` attribute must be of type {field_type}."
            raise TypeError(msg) from error
        request = self._session.subscribe_request()
        request.subscription = subscription
        response = await _send_and_wait_request(request)
        unwrap(response.result)  # Result(Void, Error)
        return DataQueue(
            path=path,
            register_function=streaming_handle.register_data_queue,
        )
