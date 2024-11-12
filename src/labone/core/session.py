"""Module for the Zurich Instruments Session Capability.

The Session capability provides access to the basic interactions with nodes
and or properties of a server. Its used in multiple places with the software
stack of Zurich Instruments. For example the Data Server kernel for a device
provides a Session capability to access the nodes of the device.

Every Session capability provides the same interface and can therefore be
handled in the same way. The only difference is the set of nodes/properties that
are available.

The number of sessions to a capability is not limited. However, due to the
asynchronous interface, it is often not necessary to have multiple sessions
to the same capability.
"""

from __future__ import annotations

import asyncio
import json
import typing as t
import uuid
from contextlib import asynccontextmanager
from enum import IntFlag
from typing import Literal

from packaging import version
from typing_extensions import NotRequired, TypeAlias, TypedDict
from zhinst.comms import unwrap

from labone.core import errors, hpk_schema
from labone.core.errors import async_translate_comms_error, translate_comms_error
from labone.core.subscription import DataQueue, QueueProtocol, StreamingHandle
from labone.core.value import AnnotatedValue, Value, value_from_python_types

if t.TYPE_CHECKING:
    import zhinst.comms

    from labone.core.helper import (
        LabOneNodePath,
        ZIContext,
    )

T = t.TypeVar("T")

# Control node for transactions. This node is only available for UHF and MF devices.
_TRANSACTION_NODE_PATH = "/ctrl/transaction/state"

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

    - Node: Node absolute path.
    - Description: Node description.
    - Properties: Comma-separated list of node properties.
        A node can have one or multiple of the following properties:

        "Read", "Write", "Stream", "Setting", "Pipelined"

    - Type: Node type.
    - Unit: Node unit.
    - Options: Possible values for the node.
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

    `ListNodesInfoFlags.SETTINGS_ONLY | ListNodesInfoFlags.EXCLUDE_VECTORS`

    - ALL: Return all matching nodes.
    - SETTINGS_ONLY: Return only setting nodes.
    - STREAMING_ONLY: Return only streaming nodes.
    - BASE_CHANNEL_ONLY: Return only one instance of a channel
            in case of multiple channels.
    - GET_ONLY: Return only nodes which can be used with the get command.
    - EXCLUDE_STREAMING: Exclude streaming nodes.
    - EXCLUDE_VECTORS: Exclude vector nodes.
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

    `ListNodesFlags.ABSOLUTE | ListNodesFlags.RECURSIVE`

    - ALL: Return all matching nodes.
    - RECURSIVE: Return nodes recursively.
    - ABSOLUTE: Absolute node paths.
    - LEAVES_ONLY: Return only leave nodes, which means they
            are at the outermost level of the three.
    - SETTINGS_ONLY: Return only setting nodes.
    - STREAMING_ONLY: Return only streaming nodes.
    - BASE_CHANNEL_ONLY: Return only one instance of a channel
            in case of multiple channels.
    - GET_ONLY: Return only nodes which can be used with the get command.
    - EXCLUDE_STREAMING: Exclude streaming nodes.
    - EXCLUDE_VECTORS: Exclude vector nodes.
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


class Session:
    """Generic Capnp session client.

    Representation of a single Session capability. This class
    encapsulates the labone interaction an exposes a python native api.
    All function are exposed as they are implemented in the interface
    of the labone server and are directly forwarded.

    Each function implements the required error handling both for the
    socket communication and the server errors. This means unless an Exception
    is raised the call was successful.

    The Session already requires an existing connection this is due to the
    fact that the instantiation is done asynchronously.

    Args:
        session: Active labone session.
        context: Context the session runs in.
    """

    # The minimum capability version that is required by the labone api.
    MIN_CAPABILITY_VERSION = version.Version("1.7.0")
    # The capability version hardcoded in the hpk schema.
    CAPABILITY_VERSION = version.Version("1.15.0")

    def __init__(
        self,
        session: zhinst.comms.DynamicClient,
        *,
        context: ZIContext,
        capability_version: version.Version,
    ):
        self._context = context
        self._session = session

        # The client_id is required by most messages to identify the client
        # on the server side. It is unique per session.
        self._client_id = uuid.uuid4()
        self._has_transaction_support: bool | None = None
        self._capability_version = capability_version

    def close(self) -> None:
        """Close the session.

        Release the underlying network resources and close the session. Since
        python does not follow the RAII pattern, it is not guaranteed that the
        destructor is called. This may causes a session to stay open even if the
        object is not used anymore.
        If needed, the close method should be called explicitly.
        """
        self._session.close()

    def ensure_compatibility(self) -> None:
        """Ensure the compatibility with the connected server.

        Ensures that all function call will work as expected and all required
        features are implemented within the server.

        Warning:
            Only the compatibility with the server is checked. The compatibility
            with the device is not checked.

        Info:
            This function is already called within the create method and does
            not need to be called again.

        Raises:
            UnavailableError: If the kernel is not compatible.
        """
        if self._capability_version < Session.MIN_CAPABILITY_VERSION:
            msg = (
                f"The data server version is not supported by the LabOne API. "
                "Please update the LabOne software to the latest version. "
                f"({self._capability_version}<{Session.MIN_CAPABILITY_VERSION})"
            )
            raise errors.UnavailableError(msg)
        if self._capability_version.major > Session.CAPABILITY_VERSION.major:
            msg = (
                "The data server version is incompatible with this LabOne API "
                "version. Please install the latest python package and retry. "
                f"({self._capability_version}> {Session.CAPABILITY_VERSION})"
            )
            raise errors.UnavailableError(msg)

    async def _list_nodes_postprocessing(
        self,
        future: t.Awaitable[hpk_schema.SessionListNodesResults],
    ) -> list[LabOneNodePath]:
        """Postprocessing for the list nodes function.

        Convert the response from the server to a list of node paths.

        Args:
            future: Future for a list nodes call.

        Returns:
            List of node paths.
        """
        response = await future
        return list(response.paths)

    @translate_comms_error
    def list_nodes(
        self,
        path: LabOneNodePath = "",
        *,
        flags: ListNodesFlags | int = ListNodesFlags.ABSOLUTE,
    ) -> t.Awaitable[list[LabOneNodePath]]:
        """List the nodes found at a given path.

        Note:
            The function is not async but returns an awaitable object.
            This makes this function eagerly evaluated instead of the python
            default lazy evaluation. This means that the request is sent to the
            server immediately even if the result is not awaited.

        Args:
            path: A string representing the path where the nodes are to be listed.
                Value is case insensitive.

                Supports * wildcards, except in the place of node path forward
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

            >>> await session.list_nodes(
            ...     "zi/devices",
            ...     flags=ListNodesFlags.RECURSIVE | ListNodesFlags.EXCLUDE_VECTORS
            ... )
        """
        future = self._session.listNodes(
            pathExpression=path,
            flags=int(flags),
            client=self._client_id.bytes,
        )
        return self._list_nodes_postprocessing(future)  # type: ignore[arg-type]

    async def _list_nodes_info_postprocessing(
        self,
        future: t.Awaitable[hpk_schema.SessionListNodesJsonResults],
    ) -> dict[LabOneNodePath, NodeInfo]:
        """Postprocessing for the list nodes info function.

        Convert the response from the server to a json dict.

        Args:
            future: Future for a list nodes call.

        Returns:
            A python dictionary of the list nodes info response.
        """
        response = await future
        try:
            return json.loads(response.nodeProps)
        except RuntimeError as e:
            msg = "Error while listing nodes info."
            raise errors.LabOneCoreError(msg) from e

    @translate_comms_error
    def list_nodes_info(
        self,
        path: LabOneNodePath = "",
        *,
        flags: ListNodesInfoFlags | int = ListNodesInfoFlags.ALL,
    ) -> t.Awaitable[dict[LabOneNodePath, NodeInfo]]:
        """List the nodes and their information found at a given path.

        Note:
            The function is not async but returns an awaitable object.
            This makes this function eagerly evaluated instead of the python
            default lazy evaluation. This means that the request is sent to the
            server immediately even if the result is not awaited.

        Args:
            path: A string representing the path where the nodes are to be listed.
                Value is case insensitive.

                Supports * wildcards, except in the place of node path
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

        Examples:
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
        future = self._session.listNodesJson(
            pathExpression=path,
            flags=int(flags),
            client=self._client_id.bytes,
        )
        return self._list_nodes_info_postprocessing(future)  # type: ignore[arg-type]

    async def _single_value_postprocessing(
        self,
        future: t.Awaitable[
            hpk_schema.SessionSetValueResults | hpk_schema.SessionGetValueResults
        ],
        path: LabOneNodePath,
    ) -> AnnotatedValue:
        """Postprocessing for the get/set with expression functions.

        Convert a single value response from the server to an annotated value.

        Args:
            future: Future for a get/set call.
            path: LabOne node path.

        Returns:
            Annotated value of the node.
        """
        response = (await future).result
        try:
            return AnnotatedValue.from_capnp(
                t.cast(hpk_schema.AnnotatedValue, unwrap(response[0])),
            )
        except IndexError as e:
            msg = f"No value returned for path {path}."
            raise errors.LabOneCoreError(msg) from e
        except RuntimeError as e:
            msg = f"Error while processing value from {path}. {e}"
            raise errors.LabOneCoreError(msg) from e

    async def _multi_value_postprocessing(
        self,
        future: t.Awaitable[
            hpk_schema.SessionSetValueResults | hpk_schema.SessionGetValueResults
        ],
        path: LabOneNodePath,
    ) -> list[AnnotatedValue]:
        """Postprocessing for the get/set with expression functions.

        Convert a multiple value response from the server to a list of
        annotated values.

        Args:
            future: Future for a get/set call.
            path: LabOne node path.

        Returns:
            List of annotated value of the node.
        """
        response = (await future).result
        try:
            return [
                AnnotatedValue.from_capnp(
                    t.cast(hpk_schema.AnnotatedValue, unwrap(raw_result)),
                )
                for raw_result in response
            ]
        except RuntimeError as e:
            msg = f"Error while setting {path}."
            raise errors.LabOneCoreError(msg) from e

    @t.overload
    def set(self, value: AnnotatedValue) -> t.Awaitable[AnnotatedValue]: ...

    @t.overload
    def set(
        self,
        value: Value,
        path: LabOneNodePath,
    ) -> t.Awaitable[AnnotatedValue]: ...

    @translate_comms_error
    def set(
        self,
        value: AnnotatedValue | Value,
        path: LabOneNodePath | None = None,
    ) -> t.Awaitable[AnnotatedValue]:
        """Set the value of a node.

        ```python
        await session.set(AnnotatedValue(path="/zi/debug/level", value=2)
        ```

        Note:
            The function is not async but returns an awaitable object.
            This makes this function eagerly evaluated instead of the python
            default lazy evaluation. This means that the request is sent to the
            server immediately even if the result is not awaited.

        Args:
            value: Value to be set.
            path: LabOne node path. The path can be relative or absolute.

        Returns:
            Acknowledged value from the device.

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
        if isinstance(value, AnnotatedValue):
            future = self._session.setValue(
                pathExpression=value.path,
                value=value_from_python_types(
                    value.value,
                    capability_version=self._capability_version,
                ),
                lookupMode="directLookup",
                client=self._client_id.bytes,
            )
            return self._single_value_postprocessing(future, value.path)  # type: ignore[arg-type]
        future = self._session.setValue(
            pathExpression=path,
            value=value_from_python_types(
                value,
                capability_version=self._capability_version,
            ),
            lookupMode="directLookup",
            client=self._client_id.bytes,
        )
        return self._single_value_postprocessing(future, path)  # type: ignore[arg-type]

    @t.overload
    def set_with_expression(
        self,
        value: AnnotatedValue,
    ) -> t.Awaitable[list[AnnotatedValue]]: ...

    @t.overload
    def set_with_expression(
        self,
        value: Value,
        path: LabOneNodePath,
    ) -> t.Awaitable[list[AnnotatedValue]]: ...

    @translate_comms_error
    def set_with_expression(
        self,
        value: AnnotatedValue | Value,
        path: LabOneNodePath | None = None,
    ) -> t.Awaitable[list[AnnotatedValue]]:
        """Set the value of all nodes matching the path expression.

        A path expression is a labone node path. The difference to a normal
        node path is that it can contain wildcards and must not be a leaf node.
        In short it is a path that can match multiple nodes. For more information
        on path expressions see the `list_nodes()` function.

        If an error occurs while fetching the values no value is returned but
        the first first exception instead.

        ```python
        ack_values = await session.set_with_expression(
                AnnotatedValue(path="/zi/*/level", value=2)
            )
        print(ack_values[0])
        ```

        Note:
            The function is not async but returns an awaitable object.
            This makes this function eagerly evaluated instead of the python
            default lazy evaluation. This means that the request is sent to the
            server immediately even if the result is not awaited.

        Args:
            value: Value to be set.
            path: LabOne node path.

        Returns:
            Acknowledged value from the device.

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
        if isinstance(value, AnnotatedValue):
            future = self._session.setValue(
                pathExpression=value.path,
                value=value_from_python_types(
                    value.value,
                    capability_version=self._capability_version,
                ),
                lookupMode="withExpansion",
                client=self._client_id.bytes,
            )
            return self._multi_value_postprocessing(future, value.path)  # type: ignore[arg-type]
        future = self._session.setValue(
            pathExpression=path,
            value=value_from_python_types(
                value,
                capability_version=self._capability_version,
            ),
            lookupMode="withExpansion",
            client=self._client_id.bytes,
        )
        return self._multi_value_postprocessing(future, path)  # type: ignore[arg-type]

    @translate_comms_error
    def get(
        self,
        path: LabOneNodePath,
    ) -> t.Awaitable[AnnotatedValue]:
        """Get the value of a node.

         The node can either be passed as an absolute path, starting with a leading
         slash and the device id (e.g. "/dev123/demods/0/enable") or as relative
         path (e.g. "demods/0/enable"). In the latter case the device id is
         automatically added to the path by the server. Note that the
         orchestrator/ZI kernel always requires absolute paths (/zi/about/version).

        ```python
        await session.get('/zi/devices/visible')
        ```

        Note:
            The function is not async but returns an awaitable object.
            This makes this function eagerly evaluated instead of the python
            default lazy evaluation. This means that the request is sent to the
            server immediately even if the result is not awaited.

        Args:
            path: LabOne node path (relative or absolute).

        Returns:
            Annotated value of the node.

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
        future = self._session.getValue(
            pathExpression=path,
            lookupMode="directLookup",
            client=self._client_id.bytes,
        )
        return self._single_value_postprocessing(future, path)  # type: ignore[arg-type]

    @translate_comms_error
    def get_with_expression(
        self,
        path_expression: LabOneNodePath,
        flags: ListNodesFlags | int = ListNodesFlags.ABSOLUTE
        | ListNodesFlags.RECURSIVE
        | ListNodesFlags.LEAVES_ONLY
        | ListNodesFlags.EXCLUDE_STREAMING
        | ListNodesFlags.GET_ONLY,
    ) -> t.Awaitable[list[AnnotatedValue]]:
        """Get the value of all nodes matching the path expression.

        A path expression is a labone node path. The difference to a normal
        node path is that it can contain wildcards and must not be a leaf node.
        In short it is a path that can match multiple nodes. For more information
        on path expressions see the `list_nodes()` function.

        If an error occurs while fetching the values no value is returned but
        the first first exception instead.

        ```python
        values = await session.get_with_expression("/zi/*/level")
        print(values[0])
        ```

        Note:
            The function is not async but returns an awaitable object.
            This makes this function eagerly evaluated instead of the python
            default lazy evaluation. This means that the request is sent to the
            server immediately even if the result is not awaited.

        Args:
            path_expression: LabOne path expression.
            flags: The flags used by the server (list_nodes()) to filter the
                nodes.

        Returns:
            Annotated values from the nodes matching the path expression.

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
        future = self._session.getValue(
            pathExpression=path_expression,
            lookupMode="withExpansion",
            flags=int(flags),
            client=self._client_id.bytes,
        )
        return self._multi_value_postprocessing(future, path_expression)  # type: ignore[arg-type]

    @t.overload
    async def subscribe(
        self,
        path: LabOneNodePath,
        *,
        queue_type: None = None,
        parser_callback: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
        get_initial_value: bool = False,
        **kwargs,
    ) -> DataQueue: ...

    @t.overload
    async def subscribe(
        self,
        path: LabOneNodePath,
        *,
        queue_type: type[QueueProtocol],
        parser_callback: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
        get_initial_value: bool = False,
        **kwargs,
    ) -> QueueProtocol: ...

    @async_translate_comms_error
    async def subscribe(
        self,
        path: LabOneNodePath,
        *,
        queue_type: type[QueueProtocol] | None = None,
        parser_callback: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
        get_initial_value: bool = False,
        **kwargs,
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

        ```python
        data_sink = await session.subscribe("/zi/devices/visible")
        newly_detected_device = await data_sink.get()
        ```

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
            kwargs: extra keyword arguments which are passed to the data-server
                to further configure the subscription.

        Returns:
            An instance of the DataQueue class. This async queue will receive
            all update events for the subscribed node.

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
        streaming_handle = StreamingHandle(parser_callback=parser_callback)

        subscription = {
            "path": path,
            "streamingHandle": self._context.register_callback(
                streaming_handle.capnp_callback,
            ),
            "subscriberId": self._client_id.bytes,
            "kwargs": {
                "entries": [
                    {
                        "key": k,
                        "value": value_from_python_types(
                            v,
                            capability_version=self._capability_version,
                        ),
                    }
                    for k, v in kwargs.items()
                ],
            },
        }
        if get_initial_value:
            _, initial_value = await asyncio.gather(
                self._session.subscribe(subscription=subscription),
                self.get(path),
            )
            new_queue_type = queue_type or DataQueue
            queue = new_queue_type(
                path=path,
                handle=streaming_handle,
            )
            queue.put_nowait(initial_value)
            return queue
        await self._session.subscribe(subscription=subscription)
        new_queue_type = queue_type or DataQueue
        return new_queue_type(
            path=path,
            handle=streaming_handle,
        )

    async def wait_for_state_change(
        self,
        path: LabOneNodePath,
        value: int,
        *,
        invert: bool = False,
    ) -> None:
        """Waits until the node has the expected state/value.

        Warning:
            Only supports integer and keyword nodes. (The value can either be the value
            or its corresponding enum value as string)

        Args:
            path: LabOne node path.
            value: Expected value of the node.
            invert: Instead of waiting for the value, the function will wait for
                any value except the passed value. (default = False)
                Useful when waiting for value to change from existing one.
        """
        # order important so that no update can happen unseen by the queue after
        # reading the current state
        queue = await self.subscribe(path, get_initial_value=True)

        # block until value is correct
        node_value = await queue.get()
        while (value != node_value.value) ^ invert:  # pragma: no cover
            node_value = await queue.get()

    async def _supports_transaction(self) -> bool:
        """Check if the session supports transactions.

        Transactions are only supported by MF and UHF devices. Transactions mitigate
        the problem of the data server being blocking. Newer devices use a different
        data server implementation which is non-blocking. Therefore transactions are
        not required for these devices.

        Returns:
            True if the session supports transactions, False otherwise.
        """
        if self._has_transaction_support is None:
            self._has_transaction_support = (
                len(await self.get_with_expression(_TRANSACTION_NODE_PATH)) == 1
            )
        return self._has_transaction_support

    @asynccontextmanager
    async def set_transaction(self) -> t.AsyncGenerator[list[t.Awaitable], None]:
        """Context manager for a transactional set.

        Once the context manager is entered, every set request that is added to the
        `requests` list is executed in a single transaction. The transaction is
        committed when the context manager is exited. Note that the transaction is
        handled by the data server and there is no special handling required by
        the client. The client can use the `set` function as usual. To ensure the
        right order of execution every promise that contains a set request must be
        added to the `requests` list. This ensures the requests are part of the
        transaction and are executed in the right order.

        By design a transaction does not return any values. This also means that
        there is no error handling for the requests. If a request fails, the other
        requests are still executed and no error is raised.

        Important:
            This function is only helpful for UHF and MF devices, since the
            underlying data server only support blocking calls. Although it can be
            used for all device types there is no benefit in this case, since it will
            be equivalent to a simple asyncio.gather.

        UHF and MF devices are not natively supported by the capnp interface of the
        LabOne data server. Therefore a wrapper is used to emulate the capnp interface.
        This wrapper uses the old LabOne Client API to communicate with the device.
        The old Client is not asynchronous and therefore the communication is blocking.
        When setting multiple nodes in a row, the blocking communication can lead to
        a significant performance drop. To avoid this, the transactional set is used.
        It allows to set multiple nodes in a single request.

        ```python
        async with session.set_transaction() as requests:
            requests.append(session.set(value1))
            requests.append(session.set(value2))
            requests.append(custom_async_function_that_sets_nodes(...))
        ```

        Yields:
            List to which the set requests must be appended.
        """
        requests: list[t.Awaitable] = []
        if await self._supports_transaction():
            begin_request = self.set(
                AnnotatedValue(path=_TRANSACTION_NODE_PATH, value=1),
            )
            yield requests
            requests = [begin_request, *requests]
            requests.append(
                self.set(AnnotatedValue(path=_TRANSACTION_NODE_PATH, value=0)),
            )
        else:
            yield requests
        await asyncio.gather(*requests)

    @property
    def context(self) -> ZIContext:
        """Get the context instance."""
        return self._context  # pragma: no cover

    @property
    def raw_session(self) -> zhinst.comms.DynamicClient:
        """Get the underlying session."""
        return self._session

    @property
    def client_id(self) -> uuid.UUID:
        """Get the underlying session."""
        return self._client_id
