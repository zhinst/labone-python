from unittest.mock import create_autospec, patch

import pytest
from labone.core import (
    KernelSession,
)
from labone.errors import LabOneError
from labone.instrument import Instrument
from labone.nodetree.node import NodeTreeManager

from tests.test_dataserver import MockModelNode


class MockInstrument(Instrument):
    def __init__(self):
        super().__init__(
            serial="serial",
            model_node=MockModelNode(),
        )


def test_unerlying_server():
    dataserver = MockInstrument()
    dataserver._tree_manager = create_autospec(NodeTreeManager)
    assert dataserver.kernel_session == dataserver._tree_manager.session


def test_repr():
    serial = "dev1234"
    mock_instrument = MockInstrument()
    mock_instrument._serial = serial
    assert serial in repr(mock_instrument)


@pytest.mark.asyncio()
async def test_connect_device():
    with patch.object(
        KernelSession,
        "create",
        autospec=True,
        return_value="session",
    ) as create_mock, patch(
        "labone.instrument.DeviceKernelInfo",
        autospec=True,
        return_value="kernel_info",
    ) as kernelinfo_mock, patch(
        "labone.instrument.ServerInfo",
        autospec=True,
        return_value="server_info",
    ) as serverinfo_mock, patch(
        "labone.instrument.Instrument",
        autospec=True,
        return_value="server_info",
    ) as init_mock, patch(
        "labone.instrument.construct_nodetree",
        autospec=True,
        return_value="node",
    ) as construct_mock:
        await Instrument.create(
            "serial",
            host="host",
            port="port",
            interface="interface",
            custom_parser="custom_parser",
        )

        kernelinfo_mock.assert_called_once_with(
            device_id="serial",
            interface="interface",
        )
        serverinfo_mock.assert_called_once_with(host="host", port="port")
        create_mock.assert_called_once_with(
            kernel_info="kernel_info",
            server_info="server_info",
        )
        construct_mock.assert_called_once_with(
            "session",
            custom_parser="custom_parser",
        )
        init_mock.assert_called_once_with(
            serial="serial",
            model_node="node",
        )


@pytest.mark.asyncio()
async def test_connect_device_raises():
    with patch.object(
        KernelSession,
        "create",
        autospec=True,
        return_value="session",
    ) as create_mock, patch(
        "labone.instrument.DeviceKernelInfo",
        autospec=True,
        return_value="kernel_info",
    ) as kernelinfo_mock, patch(
        "labone.instrument.ServerInfo",
        autospec=True,
        return_value="server_info",
    ) as serverinfo_mock, patch(
        "labone.instrument.construct_nodetree",
        autospec=True,
        return_value="node",
        side_effect=LabOneError(),
    ) as construct_mock:
        with pytest.raises(LabOneError):
            await Instrument.create(
                "serial",
                host="host",
                port="port",
                interface="interface",
                custom_parser="custom_parser",
            )
        kernelinfo_mock.assert_called_once_with(
            device_id="serial",
            interface="interface",
        )
        serverinfo_mock.assert_called_once_with(host="host", port="port")
        create_mock.assert_called_once_with(
            kernel_info="kernel_info",
            server_info="server_info",
        )
        construct_mock.assert_called_once_with(
            "session",
            custom_parser="custom_parser",
        )
