from unittest.mock import MagicMock, patch

import pytest

from labone.errors import LabOneError
from labone.instrument import Instrument
from labone.mock import AutomaticLabOneServer
from tests.mock_server_for_testing import get_mocked_node


@pytest.mark.asyncio
@patch("labone.instrument.KernelSession", autospec=True)
async def test_connect_device(kernel_session):
    session = await AutomaticLabOneServer({}).start_pipe()
    kernel_session.create.return_value = session
    instrument = await Instrument.create("dev1234", host="testee")
    assert instrument.kernel_session == session
    assert instrument.serial == "dev1234"
    kernel_session.create.assert_awaited_once()
    assert kernel_session.create.call_args.kwargs["server_info"].host == "testee"
    assert kernel_session.create.call_args.kwargs["server_info"].port == 8004


@pytest.mark.asyncio
@patch("labone.instrument.KernelSession", autospec=True)
async def test_connect_device_custom_default_args(kernel_session):
    session = await AutomaticLabOneServer({}).start_pipe()
    kernel_session.create.return_value = session
    instrument = await Instrument.create(
        "dev1234",
        host="testee",
        port=8888,
        interface="wifi",
    )
    assert instrument.kernel_session == session
    assert instrument.serial == "dev1234"
    kernel_session.create.assert_awaited_once()
    assert kernel_session.create.call_args.kwargs["server_info"].host == "testee"
    assert kernel_session.create.call_args.kwargs["server_info"].port == 8888


@pytest.mark.asyncio
@patch("labone.instrument.KernelSession", autospec=True)
async def test_connect_device_error(kernel_session):
    session = MagicMock()
    kernel_session.create.return_value = session
    session.list_nodes_info.side_effect = LabOneError()
    with pytest.raises(LabOneError):
        await Instrument.create("dev1234", host="testee")


@pytest.mark.asyncio
async def test_underlying_server():
    instrument = Instrument(serial="dev1234", model_node=await get_mocked_node({}))
    assert instrument.kernel_session == instrument._tree_manager.session


@pytest.mark.asyncio
async def test_repr():
    instrument = Instrument(serial="dev1234", model_node=await get_mocked_node({}))
    assert instrument.serial in repr(instrument)
