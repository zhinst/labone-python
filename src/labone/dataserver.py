"""High-level functionality for connecting to devices and zi-nodes."""

from __future__ import annotations

import json
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


class DataServer(PartialNode):
    """Connection to a LabOne Data Server.

    This class gives access to the LabOne Data Server configuration. It is not
    tied to a specific device but exposes the nodes used to control the
    DataServer. This is done through the so called zi-nodes.

    !!! note

        Due to the asynchronous interface, one needs to use the static method
        `create` instead of the `__init__` method.

    ```python
        from labone import DataServer
        data_server = await DataServer.create("127.0.0.1")
    ```

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
        model_node: Node,
    ):
        self._host = host
        self._port = port

        super().__init__(
            tree_manager=model_node.tree_manager,
            path_segments=model_node.path_segments,
            subtree_paths=model_node.subtree_paths,
        )

    @staticmethod
    async def create_from_session(
        *,
        session: Session,
        host: str = "localhost",
        port: int = 8004,
        custom_parser: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
        hide_zi_prefix: bool = True,
    ) -> DataServer:
        """Create a new Session to a LabOne Data Server.

        Args:
            session: Session to use for the connection.
            host: host address of the DataServer (default = "localhost").
            port: Port of the DataServer (default = 8004).
            hide_zi_prefix: Hides to common prefix `zi` from the node names.
                E.g. `data_server.debug.info` can be used instead of
                `data_server.zi.debug.info`.
            custom_parser: A function that takes an annotated value and returns an
                annotated value. This function is applied to all values coming from
                the server. It is applied after the default enum parser, if
                applicable.

        Returns:
            The connected DataServer.

        Raises:
            LabOneError: If an error appeared in the connection to the device.
        """
        try:
            model_node = await construct_nodetree(
                session,
                hide_kernel_prefix=hide_zi_prefix,
                custom_parser=custom_parser,
            )
        except LabOneError as e:
            msg = f"While connecting to DataServer at {host}:{port} an error occurred."
            raise LabOneError(msg) from e

        return DataServer(host, port, model_node=model_node)  # type: ignore[arg-type]
        # previous type ignore is due to the implicit assumption that a device root
        # will always be a partial node

    @staticmethod
    async def create(
        host: str,
        port: int = 8004,
        *,
        custom_parser: t.Callable[[AnnotatedValue], AnnotatedValue] | None = None,
        hide_zi_prefix: bool = True,
        context: ZIContext | None = None,
        timeout: int = 5000,
    ) -> DataServer:
        """Create a new Session to a LabOne Data Server.

        Args:
            host: host address of the DataServer.
            port: Port of the DataServer (default = 8004).
            hide_zi_prefix: Hides to common prefix `zi` from the node names.
                E.g. `data_server.debug.info` can be used instead of
                `data_server.zi.debug.info`.
            custom_parser: A function that takes an annotated value and returns an
                annotated value. This function is applied to all values coming from
                the server. It is applied after the default enum parser, if
                applicable.
            context: Context in which the session should run. If not provided
                the default context will be used which is in most cases the
                desired behavior.
            timeout: Timeout in milliseconds for the connection setup.

        Returns:
            The connected DataServer.

        Raises:
            UnavailableError: If the data server was not found or unable to connect.
            BadRequestError: If there is a generic problem interpreting the incoming
                request
            InternalError: If the kernel could not be launched or another internal
                error occurred.
            LabOneCoreError: If another error happens during the session creation.
            LabOneError: If an error appeared in the connection to the device.
        """
        session = await KernelSession.create(
            kernel_info=KernelInfo.zi_connection(),
            server_info=ServerInfo(host=host, port=port),
            context=context,
            timeout=timeout,
        )

        return await DataServer.create_from_session(
            session=session,
            host=host,
            port=port,
            custom_parser=custom_parser,
            hide_zi_prefix=hide_zi_prefix,
        )

    async def check_firmware_compatibility(
        self,
        devices: list[str] | None = None,
    ) -> None:
        """Check if the firmware matches the LabOne version.

        Args:
            devices: List of devices to check. If `None`, all devices connected
                to the data server are checked.

        Raises:
            ConnectionError: If the device is currently updating
            LabOneError: If the firmware revision does not match to the
                version of the connected LabOne DataServer.
        """
        raw_discovery_info = await self.tree_manager.session.get("/zi/devices")
        discovery_info: dict[str, dict] = json.loads(
            raw_discovery_info.value,  # type: ignore[arg-type]
        )

        devices_currently_updating = []
        devices_update_firmware = []
        devices_update_labone = []
        devices_to_test = devices if devices is not None else discovery_info.keys()
        for device_id, device_info in discovery_info.items():
            if device_id not in devices_to_test:
                continue
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
        return self._host  # pragma: no cover

    @property
    def port(self) -> int:
        """Port of the Data Server."""
        return self._port  # pragma: no cover

    @property
    def kernel_session(self) -> KernelSession:
        """Kernel session used by the instrument."""
        return self._tree_manager.session  # type: ignore[return-value]
