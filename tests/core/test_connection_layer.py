"""Tests for the labone.core.connection_layer module"""

import json
import socket
from unittest.mock import patch

import capnp
import pytest
from labone.core import connection_layer, errors
from labone.core.resources import (  # type: ignore[attr-defined]
    hello_msg_capnp,
    orchestrator_capnp,
)
from packaging import version


def test_open_socket_ok():
    server_info = connection_layer.ServerInfo(host="127.0.0.1", port=1234)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((server_info.host, server_info.port))
    server.listen()
    sock = connection_layer._open_socket(server_info)
    host, port = sock.getpeername()
    assert host == server_info.host
    assert port == server_info.port


def test_open_socket_non_existing():
    server_info = connection_layer.ServerInfo(host="127.0.0.1", port=1234)
    with pytest.raises(errors.LabOneConnectionError) as err:
        connection_layer._open_socket(server_info)
    assert "Connection refused" in err.value.args[0]


@pytest.fixture()
def hello_msg():
    hello_msg = hello_msg_capnp.HelloMsg.new_message()
    hello_msg.kind = hello_msg_capnp.HelloMsg.Kind.orchestrator
    hello_msg.protocol = hello_msg_capnp.HelloMsg.Protocol.http
    hello_msg.l1Ver = "99.99.99"
    hello_msg._set("schema", orchestrator_capnp.Orchestrator.capabilityVersion)
    return hello_msg


def _json_to_bytes(json_dict):
    hello_msg_raw = json.dumps(json_dict)
    hello_msg_raw = hello_msg_raw.encode("utf-8")
    return (
        hello_msg_raw
        + b" " * (hello_msg_capnp.HelloMsg.fixedLength - len(hello_msg_raw) - 1)
        + b"\x00"
    )


def _hello_msg_to_bytes(hello_msg):
    return _json_to_bytes(hello_msg.to_dict())


@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_client_handshake_ok(socket_mock, hello_msg):
    socket_mock.recv.return_value = _hello_msg_to_bytes(hello_msg)
    received_hello_msg = connection_layer._client_handshake(socket_mock)
    assert received_hello_msg.to_dict() == hello_msg.to_dict()


@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_client_handshake_invalid_json(socket_mock, hello_msg):
    socket_mock.recv.return_value = _hello_msg_to_bytes(hello_msg)[1:]
    socket_mock.getpeername.return_value = ("localhost", 1234)
    with pytest.raises(errors.LabOneConnectionError) as err:
        connection_layer._client_handshake(socket_mock)
    assert "Invalid JSON during Handshake" in err.value.args[0]


@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_client_handshake_empty_json(socket_mock):
    socket_mock.recv.return_value = _json_to_bytes({})
    socket_mock.getpeername.return_value = ("localhost", 1234)
    # capnp is able to handle an empty json but the follwoing checks will fail
    # because the default values should do not match the expected criteria
    with pytest.raises(errors.LabOneConnectionError):
        connection_layer._client_handshake(socket_mock)


@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_client_handshake_additional_fields(socket_mock, hello_msg):
    hello_msg_json = hello_msg.to_dict()
    hello_msg_json["additional_field"] = "additional_value"
    hello_msg_json["test"] = "additional_value"
    socket_mock.recv.return_value = _json_to_bytes(hello_msg_json)
    # additional fields should be filtered out an only the known field should be
    # used. This helps outputting a more helpful error message.
    received_hello_msg = connection_layer._client_handshake(socket_mock)
    assert received_hello_msg.to_dict() == hello_msg.to_dict()


def test_malicious_hello_msg_from_json():
    with pytest.raises(capnp.lib.capnp.KjException):
        connection_layer._hello_msg_from_json({"kind": "test"})


@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_client_handshake_wrong_kind(socket_mock, hello_msg):
    hello_msg.kind = hello_msg_capnp.HelloMsg.Kind.unknown
    socket_mock.recv.return_value = _hello_msg_to_bytes(hello_msg)
    socket_mock.getpeername.return_value = ("localhost", 1234)
    with pytest.raises(errors.LabOneConnectionError) as err:
        connection_layer._client_handshake(socket_mock)
    assert "Reason: Invalid server kind: unknown" in err.value.args[0]


@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_client_handshake_wrong_kind_no_check(socket_mock, hello_msg):
    hello_msg.kind = hello_msg_capnp.HelloMsg.Kind.unknown
    socket_mock.recv.return_value = _hello_msg_to_bytes(hello_msg)
    socket_mock.getpeername.return_value = ("localhost", 1234)
    received_hello_msg = connection_layer._client_handshake(socket_mock, check=False)
    assert received_hello_msg.to_dict() == hello_msg.to_dict()


@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_client_handshake_wrong_protocol(socket_mock, hello_msg):
    hello_msg.protocol = hello_msg_capnp.HelloMsg.Protocol.capnp
    socket_mock.recv.return_value = _hello_msg_to_bytes(hello_msg)
    socket_mock.getpeername.return_value = ("localhost", 1234)
    with pytest.raises(errors.LabOneConnectionError) as err:
        connection_layer._client_handshake(socket_mock)
    assert "Invalid protocol: capnp" in err.value.args[0]


@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_client_handshake_incompatible_capability_version(socket_mock, hello_msg):
    hello_msg._set("schema", "1.3.0")
    socket_mock.recv.return_value = _hello_msg_to_bytes(hello_msg)
    socket_mock.getpeername.return_value = ("localhost", 1234)
    with pytest.raises(errors.LabOneConnectionError) as err:
        connection_layer._client_handshake(socket_mock)
    assert "Unsupported LabOne Version: 99.99.99" in err.value.args[0]


@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_client_handshake_higher_capability_version(socket_mock, hello_msg):
    hello_msg._set("schema", "2.0.0")
    socket_mock.recv.return_value = _hello_msg_to_bytes(hello_msg)
    socket_mock.getpeername.return_value = ("localhost", 1234)
    with pytest.raises(errors.LabOneConnectionError) as err:
        connection_layer._client_handshake(socket_mock)
    assert "newer LabOne version" in err.value.args[0]


@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_client_handshake_illegal_capability_version(socket_mock, hello_msg):
    hello_msg._set("schema", "unknown")
    socket_mock.recv.return_value = _hello_msg_to_bytes(hello_msg)
    socket_mock.getpeername.return_value = ("localhost", 1234)
    with pytest.raises(errors.LabOneConnectionError) as err:
        connection_layer._client_handshake(socket_mock)
    assert "Unsupported LabOne Version: 99.99.99" in err.value.args[0]


def test_raise_orchestrator_error():
    error_map = {
        "ok": ValueError,
        "unknown": errors.LabOneConnectionError,
        "kernelNotFound": errors.KernelNotFoundError,
        "illegalDeviceIdentifier": errors.IllegalDeviceIdentifierError,
        "deviceNotFound": errors.DeviceNotFoundError,
        "kernelLaunchFailure": errors.KernelLaunchFailureError,
        "firmwareUpdateRequired": errors.FirmwareUpdateRequiredError,
        "interfaceMismatch": errors.InterfaceMismatchError,
        "differentInterfaceInUse": errors.DifferentInterfaceInUseError,
        "deviceInUse": errors.DeviceInUseError,
        "unsupportedApiLevel": errors.UnsupportedApiLevelError,
        "badRequest": errors.BadRequestError,
    }
    for value in orchestrator_capnp.Orchestrator.ErrorCode.schema.enumerants:
        error = orchestrator_capnp.Orchestrator.Error.new_message()
        error.code = value
        error.message = "test"
        try:
            with pytest.raises(error_map[value]) as err:
                connection_layer._raise_orchestrator_error(error)
        except KeyError as key_error:
            msg = (
                f"Error `{value}` not mapped. Please add it to the test "
                "and _raise_orchestrator_error"
            )
            raise KeyError(msg) from key_error
        if value != "ok":
            assert error.message in err.value.args[0]


@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_protocol_upgrade_ok_dev(socket_mock):
    response_lines = [
        b"HTTP/1.1 101 Switching Protocols\r\n",
        b"Connection: Upgrade\r\n",
        b"Upgrade: capnp\r\n",
        b"content-Length: 0\r\n",
        b"Zhinst-Kernel-Uid: 18c6d4e4-0a63-4a59-8c58-98a955683501\r\n",
        b"Zhinst-Kernel-Version: 1.2.3\r\n",
        b"\r\n",
    ]

    socket_mock.getpeername.return_value = ("localhost", 1234)
    socket_mock.makefile.return_value.readline.side_effect = response_lines
    kernel_info = connection_layer.DeviceKernelInfo(device_id="dev1234")

    kernel_info_extended = connection_layer._protocol_upgrade(
        socket_mock,
        kernel_info=kernel_info,
    )
    assert kernel_info_extended.name == "dev1234"
    assert kernel_info_extended.capability_version == version.Version("1.2.3")


@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_protocol_upgrade_ok_zi(socket_mock):
    response_lines = [
        b"HTTP/1.1 101 Switching Protocols\r\n",
        b"Connection: Upgrade\r\n",
        b"Upgrade: capnp\r\n",
        b"content-Length: 0\r\n",
        b"Zhinst-Kernel-Uid: 18c6d4e4-0a63-4a59-8c58-98a955683501\r\n",
        b"Zhinst-Kernel-Version: 1.2.3\r\n",
        b"\r\n",
    ]

    socket_mock.getpeername.return_value = ("localhost", 1234)
    socket_mock.makefile.return_value.readline.side_effect = response_lines
    kernel_info = connection_layer.ZIKernelInfo()

    kernel_info_extended = connection_layer._protocol_upgrade(
        socket_mock,
        kernel_info=kernel_info,
    )
    assert kernel_info_extended.name == "zi"
    assert kernel_info_extended.capability_version == version.Version("1.2.3")


@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_protocol_upgrade_ok_no_capability_version(socket_mock):
    response_lines = [
        b"HTTP/1.1 101 Switching Protocols\r\n",
        b"Connection: Upgrade\r\n",
        b"Upgrade: capnp\r\n",
        b"content-Length: 0\r\n",
        b"\r\n",
    ]

    socket_mock.getpeername.return_value = ("localhost", 1234)
    socket_mock.makefile.return_value.readline.side_effect = response_lines
    kernel_info = connection_layer.DeviceKernelInfo(
        device_id="dev1234",
        interface="1GbE",
    )

    kernel_info_extended = connection_layer._protocol_upgrade(
        socket_mock,
        kernel_info=kernel_info,
    )
    assert kernel_info_extended.name == "dev1234"
    assert kernel_info_extended.capability_version is None
    assert kernel_info_extended.device_id == kernel_info.device_id
    assert kernel_info_extended.interface == kernel_info.interface


@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_protocol_upgrade_device_not_found(socket_mock):
    response_lines = [
        b"HTTP/1.1 404 Not Found\r\n",
        b"Content-Length: 80\r\n",
        b"Content-Type: application/capnp\r\n",
        b"\r\n",
    ]

    socket_mock.getpeername.return_value = ("localhost", 1234)
    socket_mock.makefile.return_value.readline.side_effect = response_lines
    socket_mock.makefile.return_value.read.return_value = (
        b"\x00\x00\x00\x00\t\x00\x00\x00\x00\x00\x00\x00\x01\x00\x01\x00\x01"
        b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x01\x00\x04"
        b"\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x02\x01\x00\x00"
        b"No device found with id DEV123.\x00"
    )
    kernel_info = connection_layer.DeviceKernelInfo(
        device_id="dev1234",
        interface="1GbE",
    )

    with pytest.raises(errors.DeviceNotFoundError):
        connection_layer._protocol_upgrade(socket_mock, kernel_info=kernel_info)


@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_protocol_upgrade_device_different_interface(socket_mock):
    response_lines = [
        b"HTTP/1.1 409 Conflict \r\n",
        b"Content-Length: 168\r\n",
        b"Content-Type: application/capnp\r\n",
        b"\r\n",
    ]

    socket_mock.getpeername.return_value = ("localhost", 1234)
    socket_mock.makefile.return_value.readline.side_effect = response_lines
    socket_mock.makefile.return_value.read.return_value = (
        b"\x00\x00\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x01\x00\x01\x00\x01"
        b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x01\x00\x07\x00"
        b"\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\xba\x03\x00\x00"
        b"Cannot connect to DEV8563 through a USB interface. The device is "
        b"available only through the following interfaces: 1GbE\x00\x00"
    )
    kernel_info = connection_layer.DeviceKernelInfo(
        device_id="dev1234",
        interface="USB",
    )

    with pytest.raises(errors.InterfaceMismatchError):
        connection_layer._protocol_upgrade(socket_mock, kernel_info=kernel_info)


@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_protocol_upgrade_unsuported_api_level(socket_mock):
    # This is only a hypothetical scenario ... the API level always must be 6
    response_lines = [
        b"HTTP/1.1 409 Conflict \r\n",
        b"Content-Length: 56\r\n",
        b"Content-Type: application/capnp\r\n",
        b"\r\n",
    ]

    socket_mock.getpeername.return_value = ("localhost", 1234)
    socket_mock.makefile.return_value.readline.side_effect = response_lines
    socket_mock.makefile.return_value.read.return_value = (
        b"\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x01\x00\x01\x00\x01"
        b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x01\x00\n\x00\x00"
        b"\x00\x00\x00\x00\x00\x01\x00\x00\x00*\x00\x00\x00Test\x00\x00\x00\x00"
    )
    kernel_info = connection_layer.DeviceKernelInfo(
        device_id="dev1234",
        interface="USB",
    )

    with pytest.raises(errors.LabOneConnectionError):
        connection_layer._protocol_upgrade(socket_mock, kernel_info=kernel_info)


@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_protocol_upgrade_error_but_no_info(socket_mock):
    response_lines = [
        b"HTTP/1.1 409 Conflict \r\n",
        b"Content-Length: 0\r\n",
        b"Content-Type: application/capnp\r\n",
        b"\r\n",
    ]

    socket_mock.getpeername.return_value = ("localhost", 1234)
    socket_mock.makefile.return_value.readline.side_effect = response_lines
    kernel_info = connection_layer.DeviceKernelInfo(
        device_id="dev1234",
        interface="1GbE",
    )

    with pytest.raises(errors.LabOneConnectionError):
        connection_layer._protocol_upgrade(socket_mock, kernel_info=kernel_info)


@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_protocol_upgrade_not_possible(socket_mock):
    response_lines = [
        b"HTTP/1.1 200 OK\r\n",
        b"Content-Length: 104\r\n",
        b"Content-Type: application/capnp\r\n",
        b"\r\n",
    ]

    socket_mock.getpeername.return_value = ("localhost", 1234)
    socket_mock.makefile.return_value.readline.side_effect = response_lines
    socket_mock.makefile.return_value.read.return_value = (
        b"\x00\x00\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00\x01\x00\x01\x00\x00"
        b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x03\x00C\x1f\x00"
        b"\x00\x00\x00\x00\x00\t\x00\x00\x00R\x00\x00\x00\r\x00\x00\x00\x82\x00"
        b"\x00\x00\x11\x00\x00\x002\x00\x00\x00127.0.0.1\x00\x00\x00\x00\x00\x00"
        b"\x00V@b\x1eu\x07C\xee\xbf\xb3\xc0\xef\xfb\xaf0\xe40.0.0\x00\x00\x00"
    )
    kernel_info = connection_layer.DeviceKernelInfo(
        device_id="dev1234",
        interface="1GbE",
    )

    with pytest.raises(errors.LabOneConnectionError):
        connection_layer._protocol_upgrade(socket_mock, kernel_info=kernel_info)


@patch("labone.core.connection_layer._open_socket", autospec=True)
@patch("labone.core.connection_layer._client_handshake", autospec=True)
@patch("labone.core.connection_layer._protocol_upgrade", autospec=True)
def test_create_session_client_stream_ok_new_sock(
    protocol_upgrade_mock,
    handshake_mock,
    open_socket_mock,
):
    kernel_info = connection_layer.ZIKernelInfo()
    server_info = connection_layer.ServerInfo(host="localhost", port=1234)
    (
        sock,
        kernel_info_extended,
        server_info_extended,
    ) = connection_layer.create_session_client_stream(
        kernel_info=kernel_info,
        server_info=server_info,
    )
    protocol_upgrade_mock.assert_called_once()
    handshake_mock.assert_called_once()
    open_socket_mock.assert_called_once()
    assert sock == open_socket_mock.return_value
    assert kernel_info_extended == protocol_upgrade_mock.return_value
    assert server_info_extended.hello_msg == handshake_mock.return_value


@patch("labone.core.connection_layer._open_socket", autospec=True)
@patch("labone.core.connection_layer._client_handshake", autospec=True)
@patch("labone.core.connection_layer._protocol_upgrade", autospec=True)
@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_create_session_client_stream_ok_existing_sock(
    socket_mock,
    protocol_upgrade_mock,
    handshake_mock,
    open_socket_mock,
):
    socket_mock.getpeername.return_value = ("localhost", 1234)
    kernel_info = connection_layer.ZIKernelInfo()
    (
        sock,
        kernel_info_extended,
        server_info_extended,
    ) = connection_layer.create_session_client_stream(
        sock=socket_mock,
        kernel_info=kernel_info,
    )
    protocol_upgrade_mock.assert_called_once()
    handshake_mock.assert_called_once()
    # Socket exists, so it should not be opened again
    open_socket_mock.assert_not_called()
    assert sock == socket_mock
    assert kernel_info_extended == protocol_upgrade_mock.return_value
    assert server_info_extended.hello_msg == handshake_mock.return_value


@patch("labone.core.connection_layer.socket.socket", autospec=True)
def test_create_session_client_stream_both_sock_and_sock_info(socket_mock):
    kernel_info = connection_layer.ZIKernelInfo()
    server_info = connection_layer.ServerInfo(host="localhost", port=1234)
    with pytest.raises(ValueError):
        connection_layer.create_session_client_stream(
            sock=socket_mock,
            kernel_info=kernel_info,
            server_info=server_info,
        )


def test_create_session_client_stream_both_no_sock_and_no_sock_info():
    kernel_info = connection_layer.ZIKernelInfo()
    with pytest.raises(ValueError):
        connection_layer.create_session_client_stream(kernel_info=kernel_info)


@patch("labone.core.connection_layer._open_socket", autospec=True)
@patch("labone.core.connection_layer._client_handshake", autospec=True)
@patch("labone.core.connection_layer._protocol_upgrade", autospec=True)
def test_create_session_client_stream_no_handshake(
    protocol_upgrade_mock,
    handshake_mock,
    open_socket_mock,
):
    kernel_info = connection_layer.ZIKernelInfo()
    server_info = connection_layer.ServerInfo(host="localhost", port=1234)
    (
        sock,
        kernel_info_extended,
        server_info_extended,
    ) = connection_layer.create_session_client_stream(
        kernel_info=kernel_info,
        server_info=server_info,
        handshake=False,
    )
    protocol_upgrade_mock.assert_called_once()
    handshake_mock.assert_not_called()
    open_socket_mock.assert_called_once()
    assert sock == open_socket_mock.return_value
    assert kernel_info_extended == protocol_upgrade_mock.return_value
    assert server_info_extended.hello_msg is None
