from unittest.mock import ANY, create_autospec, patch

import pytest
from labone.core import (
    AnnotatedValue,
    KernelSession,
)
from labone.dataserver import DataServer
from labone.errors import LabOneError
from labone.nodetree.helper import Session
from labone.nodetree.node import NodeTreeManager


class MockModelNode:
    def __init__(self):
        self.tree_manager = "tree_manager"
        self.path_segments = "path_segments"
        self.subtree_paths = "subtree_paths"


class MockDataServer(DataServer):
    def __init__(self):
        super().__init__(host="host", port="port", model_node=MockModelNode())


def test_unerlying_server():
    dataserver = MockDataServer()
    dataserver._tree_manager = create_autospec(NodeTreeManager)
    assert dataserver.kernel_session == dataserver._tree_manager.session


@pytest.mark.asyncio()
async def test_create():
    with patch.object(
        KernelSession,
        "create",
        autospec=True,
        return_value="session",
    ) as create_mock, patch(
        "labone.dataserver.ZIKernelInfo",
        autospec=True,
        return_value="kernel_info",
    ) as kernelinfo_mock, patch(
        "labone.dataserver.ServerInfo",
        autospec=True,
        return_value="server_info",
    ) as serverinfo_mock, patch(
        "labone.dataserver.construct_nodetree",
        autospec=True,
        return_value="node",
    ) as construct_mock, patch(
        "labone.dataserver.DataServer.__init__",
        return_value=None,
        autospec=True,
    ) as init_mock, patch(
        "labone.dataserver.DataServer.__repr__",
        return_value="data_server",
        autospec=True,
    ):
        await DataServer.create(
            "host",
            "port",
            custom_parser="custom_parser",
            hide_kernel_prefix="hide_kernel_prefix",
        )
        kernelinfo_mock.assert_called_once_with()
        serverinfo_mock.assert_called_once_with(host="host", port="port")
        create_mock.assert_called_once_with(
            kernel_info="kernel_info",
            server_info="server_info",
        )
        construct_mock.assert_called_once_with(
            "session",
            hide_kernel_prefix="hide_kernel_prefix",
            custom_parser="custom_parser",
        )
        init_mock.assert_called_once_with(ANY, "host", "port", model_node="node")


@pytest.mark.asyncio()
async def test_create_raises():
    with patch.object(
        KernelSession,
        "create",
        autospec=True,
        return_value="session",
    ) as create_mock, patch(
        "labone.dataserver.ZIKernelInfo",
        autospec=True,
        return_value="kernel_info",
    ) as kernelinfo_mock, patch(
        "labone.dataserver.ServerInfo",
        autospec=True,
        return_value="server_info",
    ) as serverinfo_mock, patch(
        "labone.dataserver.construct_nodetree",
        autospec=True,
        return_value="node",
        side_effect=LabOneError(),
    ) as construct_mock, patch(
        "labone.dataserver.DataServer.__init__",
        autospec=True,
    ) as init_mock:
        with pytest.raises(LabOneError):
            await DataServer.create(
                "host",
                "port",
                custom_parser="custom_parser",
                hide_kernel_prefix="hide_kernel_prefix",
            )
        kernelinfo_mock.assert_called_once_with()
        serverinfo_mock.assert_called_once_with(host="host", port="port")
        create_mock.assert_called_once_with(
            kernel_info="kernel_info",
            server_info="server_info",
        )
        construct_mock.assert_called_once_with(
            "session",
            hide_kernel_prefix="hide_kernel_prefix",
            custom_parser="custom_parser",
        )
        init_mock.assert_not_called()


@pytest.mark.asyncio()
async def test_connect_device():
    with patch(
        "labone.dataserver.Instrument.create",
        autospec=True,
        return_value="node",
    ) as instrument_create_mock:
        dataserver = MockDataServer()
        dataserver._host = "host"
        dataserver._port = "port"

        await DataServer.connect_device(
            dataserver,
            "serial",
            interface="interface",
            custom_parser="custom_parser",
        )

        instrument_create_mock.assert_called_once_with(
            serial="serial",
            host="host",
            port="port",
            interface="interface",
            custom_parser="custom_parser",
        )


small_response = AnnotatedValue(
    value='"DEV90021":"{"STATUSFLAGS":36}',
    path="some_path",
)


@pytest.mark.parametrize(
    ("status_nr"),
    [0, 1 << 1, 1 << 2, 1 << 3, (1 << 2) + (1 << 3)],
)
@pytest.mark.asyncio()
async def test_check_firmware_compatibility(status_nr):
    response = AnnotatedValue(
        value='{"DEV90021":{"STATUSFLAGS":' + str(status_nr) + "}}",
        path="some_path",
    )

    dataserver = MockDataServer()
    dataserver._tree_manager = create_autospec(NodeTreeManager)
    dataserver._tree_manager.session = create_autospec(Session)
    dataserver._tree_manager.session.get.return_value = response

    await DataServer.check_firmware_compatibility(dataserver)
    dataserver._tree_manager.session.get.assert_called_once_with("/zi/devices")


@pytest.mark.parametrize(
    ("id_and_codes", "contained_in_error"),
    [
        ([("DEV1000", 1 << 8)], ["updating", "DEV1000"]),
        ([("DEV1000", 1 << 4)], ["update the firmware", "DEV1000"]),
        ([("DEV1000", 1 << 5)], ["update the firmware", "DEV1000"]),
        ([("DEV1000", 1 << 6)], ["update LabOne", "DEV1000"]),
        ([("DEV1000", 1 << 7)], ["update LabOne", "DEV1000"]),
        (
            [("DEV1000", ((1 << 8) + (1 << 7)))],
            ["updating", "update LabOne", "DEV1000"],
        ),
        (
            [("DEV1000", 1 << 7), ("DEV2000", 1 << 5)],
            ["update LabOne", "update the firmware", "DEV1000", "DEV2000"],
        ),
    ],
)
@pytest.mark.asyncio()
async def test_check_firmware_compatibility_raises(id_and_codes, contained_in_error):
    val = "{"
    for id_, code in id_and_codes:
        val += '"' + id_ + '":{"STATUSFLAGS":' + str(code) + "},"
    val = val[:-1] + "}"
    response = AnnotatedValue(
        value=val,
        path="some_path",
    )
    dataserver = MockDataServer()
    dataserver._tree_manager = create_autospec(NodeTreeManager)
    dataserver._tree_manager.session = create_autospec(Session)
    dataserver._tree_manager.session.get.return_value = response

    with pytest.raises(LabOneError) as e_info:
        await DataServer.check_firmware_compatibility(dataserver)
    for s in contained_in_error:
        assert s in str(e_info.value)
    dataserver._tree_manager.session.get.assert_called_once_with("/zi/devices")
