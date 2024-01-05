"""High-level functionality for connecting to devices and zi-nodes."""
from __future__ import annotations

import json
import typing as t

from labone.core import (
    AnnotatedValue,
    KernelSession,
    ServerInfo,
    ZIKernelInfo,
)
from labone.errors import LabOneError
from labone.instrument import Instrument
from labone.nodetree import construct_nodetree
from labone.nodetree.node import PartialNode


class DataServer(PartialNode):
    """Connection to a LabOne Data Server.

    This class serves as the main entry point for any connection to a
    Zurich Instrument device.
    At Zurich Instruments a server-based connectivity methodology is used.
    Server-based means that all communication between the user and the
    instrument takes place via a computer program called a server, the data
    server. (For more information on the architecture please refer to the user
    manual http://docs.zhinst.com/labone_programming_manual/introduction.html)

    Apart from the access to the data server configuration (data server node tree)
    the main purpose of this class is to create connections to one or more devices.
    The connection to a device is established by calling the `connect_device` method.

    Note:
        Due to the asynchronous interface, one needs to use the static method
        `create` instead of the `__init__` method.

    Example:
        >>> from labone import DataServer
        >>> data_server = await DataServer.create("127.0.0.1")
        >>> device = await data_server.connect_device("dev1000")

    Args:
        host: host address of the DataServer.
        port: Port of the DataServer.
        model_node: Example node that serves as a model for setting the inherited
            node attributes.
    """

    def __init__(
        self,
        host: str,
        port: int = 8004,
        *,
        model_node: PartialNode,
    ):
        self._host = host
        self._port = port

        super().__init__(
            tree_manager=model_node.tree_manager,
            path_segments=model_node.path_segments,
            subtree_paths=model_node.subtree_paths,
        )

    @classmethod
    async def create(
        cls,
        host: str,
        port: int = 8004,
        *,
        custom_parser: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
        hide_kernel_prefix: bool = True,
    ) -> DataServer:
        """Create a new Session to a LabOne Data Server.

        Args:
            host: host address of the DataServer.
            port: Port of the DataServer.
            hide_kernel_prefix: Enter a trivial first path-segment automatically.
                E.g. having the result of this function in a variable `tree`
                `tree.debug.info` can be used instead of `tree.device1234.debug.info`.
                Setting this option makes working with the tree easier.
            custom_parser: A function that takes an annotated value and returns an
                annotated value. This function is applied to all values coming from
                the server. It is applied after the default enum parser, if
                applicable.

        Returns:
            The connected DataServer.

        Raises:
            UnavailableError: If the data server was not found or unable to connect.
            BadRequestError: If there is a generic problem interpreting the incoming
                request
            InternalError: If the kernel could not be launched or another internal
                error occurred.
            LabOneCoreError: If another error happens during the session creation.
        """
        session = await KernelSession.create(
            kernel_info=ZIKernelInfo(),
            server_info=ServerInfo(host=host, port=port),
        )

        try:
            model_node = await construct_nodetree(
                session,
                hide_kernel_prefix=hide_kernel_prefix,
                custom_parser=custom_parser,
            )
        except LabOneError as e:
            msg = f"While connecting to DataServer at {host}:{port} an error occurred."
            raise LabOneError(msg) from e

        return DataServer(host, port, model_node=model_node)  # type: ignore[arg-type]
        # previous type ignore is due to the implicit assumption that a device root
        # will always be a partial node

    async def connect_device(
        self,
        serial: str,
        *,
        interface: str = "",
        custom_parser: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
    ) -> Instrument:
        """Connect to a device.

        Args:
            serial: Serial number of the device, e.g. 'dev1000'.
                The serial number can be found on the back panel of the instrument.
            interface: The interface that should be used to connect to the device.
                It is only needed if the device is accessible through multiple
                interfaces, and a specific interface should be enforced. If no value is
                provided, the data server will automatically choose an available
                interface. (default = "")
            custom_parser: A function that takes an annotated value and returns an
                annotated value. This function is applied to all values coming from
                the server. It is applied after the default enum parser, if
                applicable.

        Returns:
            The connected device.

        Raises:
            UnavailableError: If the device was not found or unable to connect.
            BadRequestError: If there is a generic problem interpreting the incoming
                request
            InternalError: If the device kernel could not be launched or another
                internal error occurred.
            LabOneCoreError: If another error happens during the session creation.
        """
        return await Instrument.create(
            serial=serial,
            host=self.host,
            port=self.port,
            interface=interface,
            custom_parser=custom_parser,
        )

    async def check_firmware_compatibility(self) -> None:
        """Check if the firmware matches the LabOne version.

        Raises:
            ConnectionError: If the device is currently updating
            LabOneError: If the firmware revision does not match to the
                version of the connected LabOne DataServer.
        """
        annotated_devices = await self.tree_manager.session.get("/zi/devices")
        devices: dict[str, dict] = json.loads(annotated_devices.value)  # type: ignore[arg-type]

        devices_currently_updating = []
        devices_update_firmware = []
        devices_update_labone = []
        for device_id, device_info in devices.items():
            status_flag = device_info["STATUSFLAGS"]

            if status_flag & 1 << 8:
                devices_currently_updating.append(device_id)

            if status_flag & 1 << 4 or status_flag & 1 << 5:
                devices_update_firmware.append(device_id)

            if status_flag & 1 << 6 or status_flag & 1 << 7:
                devices_update_labone.append(device_id)

        messages = []
        if devices_currently_updating:
            messages.append(
                f"The device(s) {', '.join(devices_currently_updating)} "
                f"is/are currently updating. "
                "Please try again after the update process is complete.",
            )

        if devices_update_firmware:
            messages.append(
                f"The Firmware of the device(s) {', '.join(devices_update_firmware)} "
                f"do/does not match the LabOne version. "
                "Please update the firmware (e.g. in the LabOne UI)",
            )

        if devices_update_labone:
            messages.append(
                f"The Firmware of the device(s) {', '.join(devices_update_labone)} "
                f"do/does not match the LabOne version. "
                "Please update LabOne to the latest version from "
                "https://www.zhinst.com/support/download-center.",
            )

        if messages:
            raise LabOneError(
                "Found these compatibility issues:\n" + "\n".join(messages),
            )

    @property
    def host(self) -> str:
        """Host of the Data Server."""
        return self._host

    @property
    def port(self) -> int:
        """Port of the Data Server."""
        return self._port

    @property
    def kernel_session(self) -> KernelSession:
        """Kernel session used by the instrument."""
        return self._tree_manager.session  # type: ignore[return-value]
