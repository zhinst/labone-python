"""AB test to ensure that the mock behaves like the real hpk in required aspects.

These tests run the same commands on a real hpk and the mock server. If the
behavior is the same, the tests pass. If the behavior is different, the test
fails. This is to ensure that the mock server behaves like the real hpk in
required aspects.

This is especially important if the hpk evolves with time. Testing against real
functionality makes sure that the mock server is up to date. Depending tests
and code still has a meaning.

To run these tests:
 * start a real hpk server on localhost:8004
 * run pytest -k mock_compatibility

By selecting the server to run on localhost:8004, it can be chosen with
which version of the hpk the mock server is compared.
"""

import io
from contextlib import redirect_stdout

import pytest

from labone.core import AnnotatedValue, KernelInfo, KernelSession, ServerInfo
from labone.core.session import ListNodesFlags, Session
from labone.mock import AutomaticLabOneServer
from labone.mock.session import MockSession


async def get_session():
    return await KernelSession.create(
        kernel_info=KernelInfo.zi_connection(),
        server_info=ServerInfo(host="localhost", port=8004),
    )


async def get_mock_session() -> MockSession:
    # this makes sure to work on same node tree as real session
    session = await get_session()
    paths_to_info = await session.list_nodes_info("*")

    return await AutomaticLabOneServer(paths_to_info).start_pipe()


def same_prints_and_exceptions_for_real_and_mock(test_function):
    """
    Calls the decorated function with both the real hpk and
    the mock version. Compares the behavior by comparing the
    printed output. Use print statements in the decorated function
    to compare the behavior!
    If exceptions are raised, it is compared that they are raised
    in both cases and are of the same kind. However, the message
    is not compared.
    """

    async def new_test_function(*args, **kwargs):
        session = await get_session()
        mock_session = await get_mock_session()
        string_output = io.StringIO()
        string_output_mock = io.StringIO()
        exception = None
        exception_mock = None
        try:
            with redirect_stdout(string_output, *args, **kwargs):
                await test_function(session)
        except Exception as e:  # noqa: BLE001
            exception = e
        try:
            with redirect_stdout(string_output_mock, *args, **kwargs):
                await test_function(mock_session)
        except Exception as e:  # noqa: BLE001
            exception_mock = e
        assert (exception is None) == (exception_mock is None)
        if exception is not None:
            assert type(exception) is type(exception_mock)
        assert string_output.getvalue() == string_output_mock.getvalue()

    return new_test_function


@pytest.mark.mock_compatibility
@pytest.mark.parametrize(
    "path",
    [
        "",
        "*",
        "/",
        "/*",
        "/zi",
        "/zi/",
        "/zi/*",
        "/zi/debug",
        "/zi/debug/level",
        "/zi/debug/*",
        "/zi/debug/level/*",
        "/zi/debug/level/*/*",
        "/a/b",  # test invalid node
    ],
)
@pytest.mark.asyncio
async def test_list_nodes_compatible(path):
    async def procedure(session):
        nodes = await session.list_nodes(
            path,
            flags=ListNodesFlags.RECURSIVE
            | ListNodesFlags.ABSOLUTE
            | ListNodesFlags.LEAVES_ONLY,
        )
        print(sorted(nodes))  # noqa: T201

    await same_prints_and_exceptions_for_real_and_mock(procedure)()


@pytest.mark.mock_compatibility
@pytest.mark.parametrize(
    "path",
    [
        "",
        "*",
        "/",
        "/*",
        "/zi",
        "/zi/",
        "/zi/*",
        "/zi/debug",
        "/zi/debug/level",
        "/zi/debug/*",
        "/zi/debug/level/*",
        "/zi/debug/level/*/*",
        "/a/b",  # test invalid node
    ],
)
@pytest.mark.asyncio
async def test_list_nodes_info_compatible(path):
    async def procedure(session):
        nodes = await session.list_nodes_info(
            path,
            flags=ListNodesFlags.RECURSIVE
            | ListNodesFlags.ABSOLUTE
            | ListNodesFlags.LEAVES_ONLY,
        )
        print(sorted(nodes))  # noqa: T201

    await same_prints_and_exceptions_for_real_and_mock(procedure)()


@pytest.mark.mock_compatibility
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/zi/debug/level",
        "/zi/debug/log",
        "/zi*",
        "/a/b",  # test invalid node
    ],
)
async def test_get_compatible(path):
    async def procedure(session):
        result = await session.get(path)
        print(result.path)  # noqa: T201

    await same_prints_and_exceptions_for_real_and_mock(procedure)()


@pytest.mark.mock_compatibility
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/zi/debug/level",
        "/zi/debug/log",
        "/a/b",  # test invalid node
    ],
)
@pytest.mark.parametrize("value", [1, 24, 0, -1])
async def test_state_keeping_compatible(path, value):
    async def procedure(session):
        await session.set(AnnotatedValue(path=path, value=value))
        result = await session.get(path)
        print(result.path, result.value, result.extra_header)  # noqa: T201

    await same_prints_and_exceptions_for_real_and_mock(procedure)()


@pytest.mark.mock_compatibility
@pytest.mark.parametrize(
    "expression",
    [
        "",
        "*",
        "/",
        "/*",
        "/zi",
        "/zi/",
        "/zi/*",
        "/zi/debug",
        "/zi/debug/level",
        "/zi/debug/*",
        "/zi/debug/level/*",
        "/zi/debug/level/*/*",
    ],
)
@pytest.mark.asyncio
async def test_get_with_expression_compatible(expression):
    async def procedure(session):
        result = await session.get_with_expression(expression)
        print(sorted([r.path for r in result]))  # noqa: T201

    await same_prints_and_exceptions_for_real_and_mock(procedure)()


@pytest.mark.mock_compatibility
@pytest.mark.parametrize(
    "expression",
    [
        "",
        "*",
        "/",
        "/*",
        "/zi",
        "/zi/",
        "/zi/*",
        "/zi/debug",
        "/zi/debug/level",
        "/zi/debug/*",
        "/zi/debug/level/*",
        "/zi/debug/level/*/*",
    ],
)
@pytest.mark.asyncio
async def test_set_with_expression_compatible(expression):
    @same_prints_and_exceptions_for_real_and_mock
    async def procedure(session: Session):
        result = await session.set_with_expression(
            AnnotatedValue(path=expression, value=17),
        )
        print(sorted([r.path for r in result]))  # noqa: T201

    await procedure()
