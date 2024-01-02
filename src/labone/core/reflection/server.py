"""Basic dynamic reflection server.

This module implements a basic dynamic reflection server. It is used to
connected to a basic Zurich Instruments reflection server. Based on the
`reflection.capnp` schema, it loads all capabilities exposed by the server and
adds them as attributes to the server instance. This allows to access the
capabilities directly through the server instance.
"""
from __future__ import annotations

import asyncio
import logging
import re
import typing as t
from functools import lru_cache

import capnp

from labone.core.errors import UnavailableError
from labone.core.helper import (
    CapnpCapability,
    CapnpStructBuilder,
    CapnpStructReader,
    ensure_capnp_event_loop,
)
from labone.core.reflection.capnp_dynamic_type_system import build_type_system
from labone.core.reflection.helper import enforce_pwd
from labone.core.reflection.parsed_wire_schema import EncodedSchema, ParsedWireSchema
from labone.core.result import unwrap

# The kj library (used by capnp) requires the PWD environment variable to be set.
# This is needed to resolve relative paths. When using python from the command
# line, the PWD variable is set automatically. However, this is not guaranteed.
# E.g. when using python from a jupyter notebook, the PWD variable is not set
# or set to `/`. This leads to a ugly warning inside the c++ code that can not
# be caught using Python's built-in warning handling mechanisms. To avoid this
# warning, we temporary set the PWD environment variable to the current working
# directory.
with enforce_pwd():
    from labone.core.reflection import (  # type: ignore[attr-defined, import-untyped]
        reflection_capnp,
    )

logger = logging.getLogger(__name__)

SNAKE_CASE_REGEX_1 = re.compile(r"(.)([A-Z][a-z]+)")
SNAKE_CASE_REGEX_2 = re.compile(r"([a-z0-9])([A-Z])")


@lru_cache(maxsize=None)
def _to_snake_case(word: str) -> str:
    """Convert camel case to snake case.

    Args:
        word: Word to convert.

    Returns:
        Converted word.
    """
    s1 = SNAKE_CASE_REGEX_1.sub(r"\1_\2", word)
    return SNAKE_CASE_REGEX_2.sub(r"\1_\2", s1).lower()


@lru_cache(maxsize=None)
def _to_camel_case(word: str) -> str:
    temp = word.split("_")
    return temp[0] + "".join(ele.title() for ele in temp[1:])


async def _fetch_encoded_schema(
    client: capnp.TwoPartyClient,
) -> tuple[int, EncodedSchema]:
    """Fetch the encoded schema from the server.

    This is done through the reflection interface of the server.

    Args:
        client: Basic capnp client.

    Returns:
        The encoded schema and the id of the bootstrap capability.

    Raises:
        LabOneUnavailableError: If the schema cannot be fetched from the server.
    """
    reflection = client.bootstrap().cast_as(reflection_capnp.Reflection)
    try:
        schema_and_bootstrap_cap = await reflection.getTheSchema()
    except capnp.lib.capnp.KjException as e:
        msg = str(
            "Unable to connect to the server. Could not fetch the schema "
            "from the server.",
        )
        raise UnavailableError(msg) from e
    server_schema = schema_and_bootstrap_cap.theSchema.theSchema
    bootstrap_capability_id = schema_and_bootstrap_cap.theSchema.typeId
    return bootstrap_capability_id, server_schema


def _maybe_unwrap(maybe_result: CapnpStructReader) -> CapnpStructReader:
    """Unwrap the result of an rpc call if possible.

    Most rpc calls return a result object. This function unwraps the result
    object if possible. If the result object is a list, then the function
    unwraps every element of the list.

    Unwrapping means that the result object is replaced by the result of the
    call. If the call was successful, then the result is returned. If the call
    failed, then the corresponding exception is raised.

    Args:
        maybe_result: The result of an rpc call.

    Returns:
        The unwrapped result.

    Raises:
        LabOneCancelledError: The request was cancelled.
        NotFoundError: The requested value or node was not found.
        OverwhelmedError: The server is overwhelmed.
        BadRequestError: The request could not be interpreted.
        UnimplementedError: The request is not implemented.
        InternalError: An internal error occurred.
        UnavailableError: The device is unavailable.
        LabOneTimeoutError: A timeout occurred on the server.
    """
    if len(maybe_result.schema.fields_list) != 1 or not hasattr(maybe_result, "result"):
        # For now we just do not handle this case ... Normally we have a single
        # result or a list of results called result
        return maybe_result
    result = maybe_result.result
    if isinstance(result, capnp.lib.capnp._DynamicListReader):  # noqa: SLF001
        return [unwrap(item) for item in result]  # pragma: no cover
    return unwrap(result)


def _maybe_wrap_interface(maybe_capability: CapnpStructReader) -> CapnpStructReader:
    """Wrap the result of an rpc call in the CapabilityWrapper if possible.

    Some rpc calls return a new capability. In order to unwrap the result of
    the new capability, it needs to be wrapped in the CapabilityWrapper.

    Args:
        maybe_capability: The result of an rpc call.

    Returns:
        The wrapped result.
    """
    if isinstance(
        maybe_capability,
        capnp.lib.capnp._DynamicCapabilityClient,  # noqa: SLF001
    ):
        return CapabilityWrapper(maybe_capability)
    return maybe_capability


class RequestWrapper:
    """Wrapper around a capnp request.

    This class is used to wrap a capnp request. Its main purpose is to
    automatically unwrap the result of the request.

    In addition it allows to access the attributes of the request in snake case.
    Since the default in Capnp is camel case, this is a bit more pythonic.

    Args:
        request: The capnp request to wrap.
    """

    def __init__(self, request: CapnpStructBuilder):
        object.__setattr__(self, "_request", request)

    async def send(self) -> CapnpStructReader:
        """Send the request via capnp and unwrap the result.

        This functions mimics the capnp send function. In addition it unwraps
        the result of the request.

        Returns:
            The unwrapped result of the request.
        """
        return _maybe_wrap_interface(
            _maybe_unwrap(await object.__getattribute__(self, "_request").send()),
        )

    def __getattr__(self, name: str):  # noqa: ANN204
        return getattr(
            object.__getattribute__(self, "_request"),
            _to_camel_case(name),
        )

    def __setattr__(self, name: str, value) -> None:  # noqa: ANN001
        setattr(
            object.__getattribute__(self, "_request"),
            _to_camel_case(name),
            value,
        )

    def __dir__(self) -> set[str]:
        original_dir = dir(object.__getattribute__(self, "_request"))
        snake_case_version = [
            _to_snake_case(name) for name in original_dir if not name.startswith("_")
        ]
        return set(snake_case_version + original_dir)


class CapabilityWrapper:
    """Wrapper around a capnp capability.

    This class is used to wrap a capnp capability. Its main purpose is to
    automatically unwrap the result of the requests.

    In addition it allows to access the attributes of the capability in snake
    case. Since the default in Capnp is camel case, this is a bit more pythonic.

    Args:
    capability: The capnp capability to wrap.
    """

    def __init__(self, capability: CapnpCapability):
        self._capability = capability

    def _send_wrapper(self, func_name: str) -> t.Callable[..., CapnpStructReader]:
        """Wrap the send function of the capability.

        This function wraps the send function of the capability. It is used to
        automatically unwrap the result of the request.

        In pycapnp all rpc calls are done through the send function. It takes
        the name of the function to call as the first argument and calls the right
        function on the server. The result of the call is returned as a capnp
        struct.

        Args:
            func_name: Name of the function to wrap.

        Returns:
            The wrapped function.
        """

        async def wrapper(*args, **kwargs) -> CapnpStructReader:
            """Generic capnp rpc call."""
            return _maybe_wrap_interface(
                _maybe_unwrap(
                    await self._capability._send(  # noqa: SLF001
                        func_name,
                        *args,
                        **kwargs,
                    ),
                ),
            )

        return wrapper

    def _request_wrapper(
        self,
        func_name: str,
    ) -> t.Callable[..., RequestWrapper | CapnpStructBuilder]:
        """Wrap the request function of the capability.

        This function wraps the request function of the capability. It is used
        to automatically unwrap the result of the request.

        A request is a capnp struct that exposes all the attributes of the
        request. The request is sent to the server by calling the `send`
        function. The result of the call is returned as a capnp struct.

        Args:
            func_name: Name of the function to wrap.

        Returns:
            The wrapped function.
        """

        def wrapper(
            *,
            apply_wrapper: bool = True,
        ) -> RequestWrapper | CapnpStructBuilder:
            """Create a capnp rpc request."""
            capnp_request = self._capability._request(func_name)  # noqa: SLF001
            return RequestWrapper(capnp_request) if apply_wrapper else capnp_request

        return wrapper

    def __getattr__(self, name: str):  # noqa: ANN204
        if name.endswith("_request"):
            name_converted = _to_camel_case(name[:-8])
            if name_converted in self._capability.schema.method_names_inherited:
                return self._request_wrapper(name_converted)
        name_converted = _to_camel_case(name)
        if name_converted in self._capability.schema.method_names_inherited:
            return self._send_wrapper(name_converted)
        return getattr(self._capability, name)

    def __dir__(self) -> set[str]:
        original_dir = dir(self._capability)
        snake_case_version = [
            _to_snake_case(name) for name in original_dir if not name.startswith("_")
        ]
        return set(snake_case_version + original_dir)

    @property
    def capnp_capability(self) -> CapnpCapability:
        """Return the underlying capnp capability."""
        return self._capability  # pragma: no cover


class ReflectionServer:
    """Basic dynamic reflection server.

    This class is used to connected to a basic Zurich Instruments reflection
    server. Based on the `reflection.capnp` schema, it loads all capabilites
    exposed by the server and adds them as attributes to the server instance.
    This allows to access the capabilities directly through the server instance.

    The ReflectionServer class is instantiated through the staticmethod
    `create()` or `create_from_connection`. This is due to the fact that the
    instantiation is done asynchronously.

    Args:
        connection: Raw capnp asyncio stream for the connection to the server.
        client: Basic capnp client.
        encoded_schema: The encoded schema of the server.
        bootstrap_capability_id: The id of the bootstrap capability.
        unwrap_result: Flag if results should be unwrapped. Most rpc calls
            return a result object. If this flag is the result of these
            calls will be unwrapped with `labone.core.result.unwrap()`.
            For more information see the documentation of the `unwrap()`.
            (default: True)
    """

    def __init__(  # noqa: PLR0913
        self,
        *,
        connection: capnp.AsyncIoStream,
        client: capnp.TwoPartyClient,
        encoded_schema: EncodedSchema,
        bootstrap_capability_id: int,
        unwrap_result: bool = True,
    ) -> None:
        self._connection = connection
        self._client = client
        self._unwrap_result = unwrap_result
        self._parsed_schema = ParsedWireSchema(encoded_schema)
        build_type_system(self._parsed_schema.full_schema, self)

        # Add to the server an instance of the bootstrap capability.
        # So for example if the server exposes a FluxDevice interface,
        # server will have "flux_device" attribute.
        bootstrap_capability_name = self._parsed_schema.full_schema[
            bootstrap_capability_id
        ].name
        instance_name = _to_snake_case(bootstrap_capability_name)

        capability = self._client.bootstrap().cast_as(
            getattr(self, bootstrap_capability_name),
        )
        setattr(
            self,
            instance_name,
            CapabilityWrapper(capability) if unwrap_result else capability,
        )

        logger.info(
            "Server exposes a %s interface. Access it with server. %s",
            bootstrap_capability_name,
            instance_name,
        )
        # Save the event loop the server was created in. This is needed to
        # close the rpc client connection in the destructor of the server.
        self._creation_loop = asyncio.get_event_loop()

    async def _close_rpc_client(self) -> None:  # pragma: no cover
        """Close the rpc client connection.

        This function is called in the destructor of the server. It closes the
        rpc client connection.

        There is a bit of a catch to this function. The capnp client does a lot
        of stuff in the background for every client. Before the server can be
        closed, the client needs to be closed. Python takes care of this
        automatically since the client is a member of the server.
        However the client MUST be closed in the same thread in which the kj
        event loop is running. If everything is done in the same thread, then
        there is not problem. However, if the kj event loop is running in a
        different thread, e.g. when using the sync wrapper, then the client
        needs to be closed in the same thread as the kj event loop. Thats why
        this function is async even though it does not need to be.
        """
        self._client.close()

    def __del__(self) -> None:  # pragma: no cover
        # call the close_rpc_client function in the event loop the server
        # was created in. See the docstring of the function for more details.
        if (
            hasattr(self, "_creation_loop")
            and self._creation_loop is not None
            and self._creation_loop.is_running()
            and asyncio.get_event_loop() != self._creation_loop
        ):
            _ = asyncio.ensure_future(  # noqa: RUF006
                self._close_rpc_client(),
                loop=self._creation_loop,
            )

    @staticmethod
    async def create(
        host: str,
        port: int,
        *,
        unwrap_result: bool = True,
    ) -> ReflectionServer:
        """Connect to a reflection server.

        Args:
            host: Host of the server.
            port: Port of the server.
            unwrap_result: Flag if results should be unwrapped. Most rpc calls
                return a result object. If this flag is the result of these
                calls will be unwrapped with `labone.core.result.unwrap()`.
                For more information see the documentation of the `unwrap()`.
                (default: True)


        Returns:
            The reflection server instance.
        """
        await ensure_capnp_event_loop()
        connection = await capnp.AsyncIoStream.create_connection(host=host, port=port)
        return await ReflectionServer.create_from_connection(
            connection,
            unwrap_result=unwrap_result,
        )

    @staticmethod
    async def create_from_connection(
        connection: capnp.AsyncIoStream,
        *,
        unwrap_result: bool = True,
    ) -> ReflectionServer:
        """Create a reflection server from an existing connection.

        Args:
            connection: Raw capnp asyncio stream for the connection to the server.
            unwrap_result: Flag if results should be unwrapped. Most rpc calls
                return a result object. If this flag is the result of these
                calls will be unwrapped with `labone.core.result.unwrap()`.
                For more information see the documentation of the `unwrap()`.
                (default: True)

        Returns:
            The reflection server instance.
        """
        client = capnp.TwoPartyClient(connection)
        (
            bootstrap_capability_id,
            encoded_schema,
        ) = await _fetch_encoded_schema(client)
        return ReflectionServer(
            connection=connection,
            client=client,
            encoded_schema=encoded_schema,
            bootstrap_capability_id=bootstrap_capability_id,
            unwrap_result=unwrap_result,
        )
