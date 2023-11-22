"""Base Instrument Driver.

Natively works with all device types.
"""
from __future__ import annotations

import typing as t

from labone.core import (
    AnnotatedValue,
    DeviceKernelInfo,
    KernelSession,
    ServerInfo,
)
from labone.errors import LabOneError
from labone.nodetree import construct_nodetree
from labone.nodetree.node import Node, PartialNode


class Instrument(PartialNode):
    """Generic driver for a Zurich Instrument device.

    Note: It is implicitly assumed that the device is not a leaf node and does
        not contain wildcards.

    Args:
        serial: Serial number of the device, e.g. 'dev1000'.
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
            path_aliases=model_node.path_aliases,
        )

    @staticmethod
    async def create(  # noqa: PLR0913
        serial: str,
        *,
        host: str,
        port: int,
        interface: str = "",
        use_enum_parser: bool = True,
        custom_parser: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
    ) -> Instrument:
        """Connect to a device.

        Args:
            serial: Serial number of the device, e.g. 'dev1000'.
                The serial number can be found on the back panel of the instrument.
            host: host address of the DataServer.
            port: Port of the DataServer.
            interface: The interface that should be used to connect to the device.
                It is only needed if the device is accessible through multiple
                interfaces, and a specific interface should be enforced. If no value is
                provided, the data server will automatically choose an available
                interface. (default = "")
            use_enum_parser: Whether enumerated integer values coming from the server
                should be packaged into enum values, if applicable.
            custom_parser: A function that takes an annotated value and returns an
                annotated value. This function is applied to all values coming from
                the server. It is applied after the default enum parser, if
                applicable.

        Returns:
            The connected device.

        Raises:
            LabOneError: If an error appeared in the connection to the device.
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
        session = await KernelSession.create(
            kernel_info=DeviceKernelInfo(device_id=serial, interface=interface),
            server_info=ServerInfo(host=host, port=port),
        )

        try:
            model_node = await construct_nodetree(
                session,
                use_enum_parser=use_enum_parser,
                custom_parser=custom_parser,
            )
        except LabOneError as e:
            msg = (
                f"While connecting to device {serial} through {interface},"
                f" an error occured."
            )
            raise LabOneError(msg) from e

        return Instrument(
            serial=serial,
            model_node=model_node,
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
