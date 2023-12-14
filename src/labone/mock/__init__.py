"""Mock Server.

A capnp server is provided, which will run locally. An interface is provided for
defining the behavior of the server. Subclassing this interface allows for
custom mock server definition. An example implementation is provided defining
typical desired behavior. This way a custom implementation does not need to start
from scratch.

Example:
    >>> mock_session = await spawn_hpk_mock(
            AutomaticSessionFunctionality(paths_to_info)
        )

    >>> queue = await session.subscribe("/a/b/c")
    >>> print(await session.set(AnnotatedValue(path="/a/b/c", value=123, timestamp=0)))
    >>> print(await session.get("/a/b/t"))
"""

from labone.mock.automatic_session_functionality import AutomaticSessionFunctionality
from labone.mock.entry_point import spawn_hpk_mock
from labone.mock.session_mock_template import SessionMockFunctionality

__all__ = [
    "spawn_hpk_mock",
    "AutomaticSessionFunctionality",
    "SessionMockFunctionality",
]
