"""Simplifying the creation of a mock server."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from labone.core.reflection.server import ReflectionServer
from labone.core.session import Session
from labone.mock.mock_server import MockServer
from labone.mock.session_mock_template import SessionMockTemplate

if TYPE_CHECKING:
    from labone.core.helper import CapnpCapability
    from labone.mock.session_mock_template import SessionMockFunctionality

SESSION_REFLECTION_BIN = Path(__file__).parent.parent / "resources" / "session.bin"


class MockSession(Session):
    """Regular Session holding a mock server.

    This class is designed for holding the mock server.
    This is needed, because otherwise,
    there would be no reference to the capnp objects, which would go out of scope.
    This way, the correct lifetime of the capnp objects is ensured, by attaching it to
    its client.
    """

    def __init__(
        self,
        mock_server: MockServer,
        capnp_session: CapnpCapability,
        *,
        reflection_server: ReflectionServer,
    ):
        super().__init__(capnp_session, reflection_server=reflection_server)
        self._mock_server = mock_server


async def spawn_hpk_mock(
    functionality: SessionMockFunctionality,
) -> MockSession:
    """Shortcut for creating a mock server.

    ```python
    mock_server = await spawn_hpk_mock(
            AutomaticSessionFunctionality(paths_to_info)
        )
    ```

    Args:
        functionality: Functionality to be mocked.

    Returns:
        Mock server.

    Raises:
        FileNotFoundError: If the file does not exist.
        PermissionError: If the file cannot be read.
        capnp.lib.capnp.KjException: If the schema is invalid. Or the id
            of the concrete server is not in the schema.
    """
    mock_server = MockServer(
        capability_bytes=SESSION_REFLECTION_BIN,
        concrete_server=SessionMockTemplate(functionality),
    )
    client_connection = await mock_server.start()
    reflection_client = await ReflectionServer.create_from_connection(client_connection)
    return MockSession(
        mock_server,
        reflection_client.session,  # type: ignore[attr-defined]
        reflection_server=reflection_client,
    )
