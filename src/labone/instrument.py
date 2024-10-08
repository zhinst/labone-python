"""Base Instrument Driver.

Natively works with all device types.
"""

from __future__ import annotations

import typing as t

from labone.core import (
    AnnotatedValue,
    KernelInfo,
    KernelSession,
    ServerInfo,
    Session,
    ZIContext,
)
from labone.errors import LabOneError
from labone.nodetree import construct_nodetree
from labone.nodetree.node import Node, PartialNode

if t.TYPE_CHECKING:
    from labone.core.errors import (  # noqa: F401
        BadRequestError,
        InternalError,
        LabOneCoreError,
        UnavailableError,
    )


class Instrument(PartialNode):
    """Generic driver for a Zurich Instrument device.

    This class serves as the main entry point for any connection to a
    Zurich Instrument device.

    At Zurich Instruments a server-based connectivity methodology is used.
    Server-based means that all communication between the user and the
    instrument takes place via a computer program called a server, the data
    server. (For more information on the architecture please refer to the
    [user manual](http://docs.zhinst.com/labone_programming_manual/introduction.html))

    !!! note

        Due to the asynchronous interface, one needs to use the static method
        `create` instead of the `__init__` method.

    ```python
    from labone import Instrument
    instrument = await Instrument.create("dev2345", host="127.0.0.1")
    ```

    Args:
        serial: Serial number of the device, e.g. 'dev2345'.
            The serial number can be found on the back panel of the instrument.
        model_node: Example node which serves as a model for setting the inherited
            node attributes.
    """

    def __init__(
        self,
        *,
        serial: str,
        model_node: Node,
    ):
        self._serial = serial
        super().__init__(
            tree_manager=model_node.tree_manager,
            path_segments=model_node.path_segments,
            subtree_paths=model_node.subtree_paths,
        )

    @staticmethod
    async def create_from_session(
        serial: str,
        *,
        session: Session,
        custom_parser: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
    ) -> Instrument:
        """Create an Instrument from an existing session.

        Args:
            serial: Serial number of the device, e.g. `dev2345`.
                The serial number can be found on the back panel of the instrument.
            session: Session to use for the instrument.
            custom_parser: A function that takes an annotated value and returns an
                annotated value. This function is applied to all values coming from
                the server. It is applied after the default enum parser, if
                applicable.

        Returns:
            The connected device.

        Raises:
            LabOneError: If an error appeared in the connection to the device.
        """
        try:
            model_node = await construct_nodetree(
                session,
                custom_parser=custom_parser,
            )
        except LabOneError as e:
            msg = f"While connecting to device {serial} an error occurred."
            raise LabOneError(msg) from e

        return Instrument(
            serial=serial,
            model_node=model_node,
        )

    @staticmethod
    async def create(
        serial: str,
        *,
        host: str,
        port: int = 8004,
        interface: str = "",
        custom_parser: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
        context: ZIContext | None = None,
        timeout: int = 5000,
    ) -> Instrument:
        """Connect to a device.

        Args:
            serial: Serial number of the device, e.g. `dev2345`.
                The serial number can be found on the back panel of the instrument.
            host: host address of the DataServer.
            port: Port of the DataServer.
            interface: The interface that should be used to connect to the device.
                It is only needed if the device is accessible through multiple
                interfaces, and a specific interface should be enforced. If no value is
                provided, the data server will automatically choose an available
                interface. (default = "")
            custom_parser: A function that takes an annotated value and returns an
                annotated value. This function is applied to all values coming from
                the server. It is applied after the default enum parser, if
                applicable.
            context: Context in which the session should run. If not provided
                the default context will be used which is in most cases the
                desired behavior.
            timeout: Timeout in milliseconds for the connection setup.

        Returns:
            The connected device.

        Raises:
            UnavailableError: If the kernel was not found or unable to connect.
            BadRequestError: If there is a generic problem interpreting the incoming
                request
            InternalError: If the kernel could not be launched or another internal
                error occurred.
            LabOneCoreError: If another error happens during the session creation.
            LabOneError: If an error appeared in the connection to the device.
        """
        session = await KernelSession.create(
            kernel_info=KernelInfo.device_connection(
                device_id=serial,
                interface=interface,
            ),
            server_info=ServerInfo(host=host, port=port),
            context=context,
            timeout=timeout,
        )
        return await Instrument.create_from_session(
            serial,
            session=session,
            custom_parser=custom_parser,
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.serial})"

    @property
    def serial(self) -> str:
        """Instrument specific serial."""
        return self._serial

    @property
    def kernel_session(self) -> KernelSession:
        """Kernel session used by the instrument."""
        return self._tree_manager.session  # type: ignore[return-value]
