"""
To run these tests:
 * start a real hpk server on localhost:8004
 * start hwmock and write serial below
"""
HWMOCK_SERIAL = "dev90037"
"""
 * run: pytest -k mock_compatibility_hwmock

"""


import io
from contextlib import redirect_stdout
import numpy as np

import pytest
from labone.core import AnnotatedValue, KernelSession, ServerInfo, ZIKernelInfo
from labone.core.connection_layer import DeviceKernelInfo
from labone.core.session import ListNodesFlags, Session
from labone.mock import spawn_hpk_mock
from labone.mock.automatic_session_functionality import AutomaticSessionFunctionality
from labone.mock.entry_point import MockSession
from tests.mock.ab_hpk_automatic_functionality_test import create_compare_function


async def get_hwmock_session(serial: str):
    return await KernelSession.create(
        kernel_info=DeviceKernelInfo(device_id=serial),
        server_info=ServerInfo(host="localhost", port=8004),
    )


async def get_mock_session_like_hwmock() -> MockSession:
    # this makes sure to work on same node tree as real session
    session = await get_hwmock_session(HWMOCK_SERIAL)
    paths_to_info = await session.list_nodes_info("*")
    functionality = AutomaticSessionFunctionality(paths_to_info)
    return await spawn_hpk_mock(functionality)


same_prints_and_exceptions_for_hwmock_and_mock = create_compare_function(
    lambda: get_hwmock_session(HWMOCK_SERIAL), get_mock_session_like_hwmock)


@pytest.mark.mock_compatibility()
@pytest.mark.asyncio()
@pytest.mark.parametrize(
    "path",
    [
        "/a/b",  # test invalid node
        '/dev90037/hwmock/scalars/1/double',  # todo: somehow make independend of dev id
        '/dev90037/hwmock/scalars/1/float',
        '/dev90037/hwmock/scalars/1/complex',
        '/dev90037/hwmock/scalars/1/uint64',
        '/dev90037/hwmock/scalars/1/int32',
        '/dev90037/hwmock/scalars/1/uint32',
        '/dev90037/hwmock/sweepshf/data/0/plainwave',

    ],
)
@pytest.mark.parametrize("value", [1, 24, 0, -1, "abc", "trace", 1+2j, True, np.array([1, 2, 3])])
async def test_set_compatible(path, value):
    async def procedure(session):
        result = await session.set(
            AnnotatedValue(path=path, value=value),
        )
        print(result.path, result.value, result.extra_header)

    await same_prints_and_exceptions_for_hwmock_and_mock(procedure)()