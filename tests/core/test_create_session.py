"""Tests the creation of a labone.core.session.Session object"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from labone.core import connection_layer, session
from labone.core.resources import (  # type: ignore[attr-defined]
    session_protocol_capnp,
)


@patch("labone.core.session.create_session_client_stream", autospec=True)
@patch("labone.core.session.capnp", autospec=True)
@pytest.mark.asyncio()
async def test_session_create_ok_zi(capnp_mock, create_session_client_stream):
    dummy_sock = MagicMock()
    dummy_kernel_info_extended = MagicMock()
    dummy_server_info_extended = MagicMock()
    create_session_client_stream.return_value = (
        dummy_sock,
        dummy_kernel_info_extended,
        dummy_server_info_extended,
    )
    capnp_mock.AsyncIoStream.create_connection = AsyncMock()
    kernel_info = connection_layer.ZIKernelInfo()
    server_info = connection_layer.ServerInfo(host="localhost", port=8004)
    created_session = await session.Session.create(
        kernel_info=kernel_info,
        server_info=server_info,
    )

    create_session_client_stream.assert_called_once_with(
        kernel_info=kernel_info,
        server_info=server_info,
    )
    capnp_mock.AsyncIoStream.create_connection.assert_called_once_with(sock=dummy_sock)
    capnp_mock.TwoPartyClient.assert_called_once_with(
        capnp_mock.AsyncIoStream.create_connection.return_value,
    )
    capnp_mock.TwoPartyClient().bootstrap().cast_as.assert_called_once_with(
        session_protocol_capnp.Session,
    )

    assert (
        created_session._session
        == capnp_mock.TwoPartyClient().bootstrap().cast_as.return_value
    )
    assert created_session._client == capnp_mock.TwoPartyClient.return_value
    assert created_session.kernel_info == dummy_kernel_info_extended
    assert created_session.server_info == dummy_server_info_extended
