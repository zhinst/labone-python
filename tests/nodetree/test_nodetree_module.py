import asyncio

import pytest
from labone.core import KernelSession, ServerInfo, ZIKernelInfo
from labone.nodetree import construct_nodetree

USE_REAL_SESSION = 0


@pytest.mark.asyncio()
async def test_use_case(session_mock):
    if USE_REAL_SESSION:
        session = await KernelSession.create(
            kernel_info=ZIKernelInfo(),
            server_info=ServerInfo(host="localhost", port=8004),
        )
    zi = await construct_nodetree(session=session if USE_REAL_SESSION else session_mock)

    assert (await zi.debug.level(5)).value == 5
    assert (await zi.debug.level()).value == 5

    result_node = await zi.devices()

    assert result_node.visible.value == ""
    assert result_node.connected.value == ""

    await zi["*"].groups[0].status(0)
    result_node = await zi["*/groups"]()

    for sub in result_node:
        assert sub in result_node

    # set to this before, see above
    result_node[0][0].status = 0

    # set to this before, see above
    await zi.debug.level.wait_for_state_change(5, timeout=0.01)

    with pytest.raises(asyncio.TimeoutError):
        await zi.debug.level.wait_for_state_change(3, timeout=0.01)

    node = zi.debug.level

    await asyncio.gather(
        node.wait_for_state_change(3, timeout=0.01),
        node(3),
    )
