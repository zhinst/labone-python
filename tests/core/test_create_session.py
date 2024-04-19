"""Tests the creation of a labone.core.session.Session object"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from labone.core import connection_layer, kernel_session
from labone.core.errors import LabOneCoreError, UnavailableError


@patch("labone.core.session._send_and_wait_request", autospec=True)
@patch("labone.core.kernel_session.create_session_client_stream", autospec=True)
@patch("labone.core.kernel_session.capnp", autospec=True)
@patch("labone.core.kernel_session.ReflectionServer", autospec=True)
@pytest.mark.asyncio()
async def test_session_create_ok_zi(
    reflection_server,
    capnp_mock,
    create_session_client_stream,
    send_request,
):
    dummy_sock = MagicMock()
    dummy_kernel_info_extended = MagicMock()
    dummy_server_info_extended = MagicMock()
    create_session_client_stream.return_value = (
        dummy_sock,
        dummy_kernel_info_extended,
        dummy_server_info_extended,
    )
    send_request.return_value.version = str(
        kernel_session.KernelSession.TESTED_CAPABILITY_VERSION,
    )
    capnp_mock.AsyncIoStream.create_connection = AsyncMock()
    kernel_info = connection_layer.ZIKernelInfo()
    server_info = connection_layer.ServerInfo(host="localhost", port=8004)
    created_session = await kernel_session.KernelSession.create(
        kernel_info=kernel_info,
        server_info=server_info,
    )

    create_session_client_stream.assert_called_once_with(
        kernel_info=kernel_info,
        server_info=server_info,
    )
    capnp_mock.AsyncIoStream.create_connection.assert_called_once_with(sock=dummy_sock)
    reflection_server.create_from_connection.assert_called_once()

    original_capnp_session = (
        reflection_server.create_from_connection.return_value.session.capnp_capability
    )
    assert created_session._session == original_capnp_session
    assert created_session.kernel_info == dummy_kernel_info_extended
    assert created_session.server_info == dummy_server_info_extended


@patch("labone.core.kernel_session.create_session_client_stream", autospec=True)
@patch("labone.core.kernel_session.capnp", autospec=True)
@patch("labone.core.kernel_session.ReflectionServer", autospec=True)
@pytest.mark.asyncio()
async def test_session_create_err_zi(
    reflection_server,
    capnp_mock,
    create_session_client_stream,
):
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

    reflection_server.create_from_connection.side_effect = LabOneCoreError("Test")

    with pytest.raises(UnavailableError):
        await kernel_session.KernelSession.create(
            kernel_info=kernel_info,
            server_info=server_info,
        )
