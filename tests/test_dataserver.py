from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from labone.core import (
    KernelSession,
)
from labone.dataserver import DataServer
from labone.errors import LabOneError
from labone.mock import AutomaticLabOneServer


@pytest.mark.asyncio
async def test_create_ok():
    session = await AutomaticLabOneServer({}).start_pipe()
    dataserver = await DataServer.create_from_session(
        session=session,
        host="host",
        port=8004,
    )
    assert dataserver.kernel_session == session


@pytest.mark.asyncio
async def test_create_ok_new_session():
    session = await AutomaticLabOneServer({}).start_pipe()
    with patch.object(
        KernelSession,
        "create",
        autospec=True,
        return_value="session",
    ) as create_mock:
        create_mock.return_value = session
        dataserver = await DataServer.create(host="host", port=8004)
    assert dataserver.kernel_session == session
    assert create_mock.call_count == 1


@pytest.mark.asyncio
async def test_create_raises():
    session = MagicMock()
    session.list_nodes_info = AsyncMock(side_effect=LabOneError())
    with pytest.raises(LabOneError):
        await DataServer.create_from_session(session=session, host="host", port=8004)


@pytest.mark.parametrize(
    ("status_nr"),
    [0, 1 << 1, 1 << 2, 1 << 3, (1 << 2) + (1 << 3)],
)
@pytest.mark.asyncio
async def test_check_firmware_compatibility(status_nr):
    session = await AutomaticLabOneServer({"/zi/devices": {}}).start_pipe()
    dataserver = await DataServer.create_from_session(
        session=session,
        host="host",
        port=8004,
    )
    await dataserver.devices('{"DEV90021":{"STATUSFLAGS":' + str(status_nr) + "}}")

    await DataServer.check_firmware_compatibility(dataserver)


@pytest.mark.asyncio
async def test_check_firmware_compatibility_single_instrument():
    session = await AutomaticLabOneServer({"/zi/devices": {}}).start_pipe()
    dataserver = await DataServer.create_from_session(
        session=session,
        host="host",
        port=8004,
    )
    await dataserver.devices(
        '{"DEV90021":{"STATUSFLAGS": 0 },"DEV90022":{"STATUSFLAGS": 16 }}',
    )
    await DataServer.check_firmware_compatibility(dataserver, devices=["DEV90021"])


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
@pytest.mark.asyncio
async def test_check_firmware_compatibility_raises(id_and_codes, contained_in_error):
    val = "{"
    for id_, code in id_and_codes:
        val += '"' + id_ + '":{"STATUSFLAGS":' + str(code) + "},"
    val = val[:-1] + "}"
    session = await AutomaticLabOneServer({"/zi/devices": {}}).start_pipe()
    dataserver = await DataServer.create_from_session(
        session=session,
        host="host",
        port=8004,
    )
    await dataserver.devices(val)

    with pytest.raises(LabOneError) as e_info:
        await DataServer.check_firmware_compatibility(dataserver)
    for s in contained_in_error:
        assert s in str(e_info.value)
