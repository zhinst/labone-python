"""Mock Server.

A capnp server is provided, which will run locally. An interface is provided for
defining the behavior of the server. Subclassing this interface allows for
custom mock server definition. An example implementation is provided defining
typical desired behavior. This way a custom implementation does not need to start
from scratch.

Example:
    >>> mock_session = await AutomaticLabOneServer.start_pipe(paths_to_info)
    >>> queue = await session.subscribe("/a/b/c")
    >>> print(await session.set(AnnotatedValue(path="/a/b/c", value=123, timestamp=0)))
    >>> print(await session.get("/a/b/t"))
"""

from labone.mock.automatic_server import AutomaticLabOneServer

__all__ = [
    "AutomaticLabOneServer",
]
