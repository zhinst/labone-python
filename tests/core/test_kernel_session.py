from unittest.mock import AsyncMock, Mock, patch

import pytest

from labone.core import KernelInfo, KernelSession, ServerInfo


@pytest.mark.asyncio()
@patch("zhinst.comms.CapnpContext")
async def test_kernel_session(context):
    session = Mock()
    context.connect_labone = AsyncMock(return_value=session)
    kernel_info = KernelInfo.zi_connection()
    kernel_session = await KernelSession.create(
        kernel_info=kernel_info,
        server_info=ServerInfo(host="localhost", port=8004),
        context=context,
    )
    assert kernel_session.raw_session == session
    assert context.connect_labone.await_count == 1
    context.connect_labone.assert_awaited_once()
    assert kernel_session.server_info.host == "localhost"
    assert kernel_session.server_info.port == 8004
