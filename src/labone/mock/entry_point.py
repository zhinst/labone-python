"""Simplifying the creation of a mock server."""

from __future__ import annotations

from typing import TYPE_CHECKING

from labone.core.helper import CapnpLock, create_lock
from labone.core.reflection.server import ReflectionServer
from labone.core.session import Session
from labone.mock.hpk_schema import get_schema
from labone.mock.mock_server import start_local_mock
from labone.mock.session_mock_template import SessionMockTemplate

if TYPE_CHECKING:
    import capnp

    from labone.core.helper import CapnpCapability
    from labone.mock.session_mock_template import SessionMockFunctionality


class MockSession(Session):
    """Regular Session holding a mock server.

    This class is designed for holding the mock server.
    This is needed, because otherwise,
    there would be no reference to the capnp objects, which would go out of scope.
    This way, the correct lifetime of the capnp objects is ensured, by attaching it to
    its client.

    Args:
        mock_server: Mock server.
        capnp_session: Capnp session.
        reflection: Reflection server.
        capnp_lock: Capnp lock.
    """

    def __init__(
        self,
        mock_server: capnp.TwoPartyServer,
        capnp_session: CapnpCapability,
        *,
        reflection: ReflectionServer,
        capnp_lock: CapnpLock,
    ):
        super().__init__(
            capnp_session,
            reflection_server=reflection,
            capnp_lock=capnp_lock,
        )
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
    server, client = await start_local_mock(
        schema=get_schema(),
        mock=SessionMockTemplate(functionality),
    )
    reflection = await ReflectionServer.create_from_connection(client)
    capnp_lock = await create_lock()
    return MockSession(
        server,
        reflection.session,  # type: ignore[attr-defined]
        reflection=reflection,
        capnp_lock=capnp_lock,
    )
