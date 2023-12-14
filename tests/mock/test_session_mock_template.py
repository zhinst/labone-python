"""
Scope: session, capnp, mock server, concrete (hpk) mock server

The desired properties of the session mock template are that
a call to a session will result in a corresponding call to the functionality.

As the functionality is seperated via an interface, we test that this
interface will be called correctly. Thus we show, that session, capnp and
server mock are compatible and work as a functional unit.
"""


from unittest.mock import ANY, Mock

import pytest
from labone.core.session import ListNodesFlags
from labone.core.value import AnnotatedValue
from labone.mock.entry_point import spawn_hpk_mock
from labone.mock.session_mock_template import SessionMockFunctionality


@pytest.mark.asyncio()
async def test_set_propagates_to_functionality_and_back():
    functionality = Mock(spec=SessionMockFunctionality)
    functionality.set.return_value = AnnotatedValue(
        path="/mock/path",
        value=1,
        timestamp=2,
    )

    session = await spawn_hpk_mock(functionality)

    response = await session.set(AnnotatedValue(path="/a/b", value=7))

    # propogates to functionality
    functionality.set.assert_called_once_with(AnnotatedValue(path="/a/b", value=7))

    # returns functionality response
    assert response == AnnotatedValue(path="/mock/path", value=1, timestamp=2)


@pytest.mark.asyncio()
async def test_get_propagates_to_functionality_and_back():
    functionality = Mock(spec=SessionMockFunctionality)
    functionality.get.return_value = AnnotatedValue(
        path="/mock/path",
        value=1,
        timestamp=2,
    )

    session = await spawn_hpk_mock(functionality)

    response = await session.get("/a/b")

    # propogates to functionality
    functionality.get.assert_called_once_with("/a/b")

    # returns functionality response
    assert response == AnnotatedValue(path="/mock/path", value=1, timestamp=2)


@pytest.mark.asyncio()
async def test_get_with_expression_propagates_to_functionality_and_back():
    functionality = Mock(spec=SessionMockFunctionality)
    functionality.get_with_expression.return_value = [
        AnnotatedValue(path="/mock/path", value=1, timestamp=2),
        AnnotatedValue(path="/mock/path/2", value=3, timestamp=4),
    ]

    session = await spawn_hpk_mock(functionality)

    response = await session.get_with_expression("/a/b")

    # propogates to functionality. Note: no guarantees for flags tested!
    functionality.get_with_expression.assert_called_once_with("/a/b", flags=ANY)

    # returns functionality response
    assert response == [
        AnnotatedValue(path="/mock/path", value=1, timestamp=2),
        AnnotatedValue(path="/mock/path/2", value=3, timestamp=4),
    ]


@pytest.mark.asyncio()
async def test_set_with_expression_propagates_to_functionality_and_back():
    functionality = Mock(spec=SessionMockFunctionality)
    functionality.set_with_expression.return_value = [
        AnnotatedValue(path="/mock/path", value=1, timestamp=2),
        AnnotatedValue(path="/mock/path/2", value=1, timestamp=4),
    ]

    session = await spawn_hpk_mock(functionality)

    response = await session.set_with_expression(AnnotatedValue(path="/a/b", value=7))

    # propogates to functionality
    functionality.set_with_expression.assert_called_once_with(
        AnnotatedValue(path="/a/b", value=7),
    )

    # returns functionality response
    assert response == [
        AnnotatedValue(path="/mock/path", value=1, timestamp=2),
        AnnotatedValue(path="/mock/path/2", value=1, timestamp=4),
    ]


@pytest.mark.asyncio()
async def test_subscribe_propagates_to_functionality():
    functionality = Mock(spec=SessionMockFunctionality)

    session = await spawn_hpk_mock(functionality)

    await session.subscribe("/a/b")

    # propogates to functionality
    functionality.subscribe_logic.assert_called_once_with(
        path="/a/b",
        streaming_handle=ANY,
        subscriber_id=ANY,
    )


@pytest.mark.asyncio()
async def test_list_nodes_propagates_to_functionality_and_back():
    functionality = Mock(spec=SessionMockFunctionality)
    functionality.list_nodes.return_value = ["/a/b", "/a/c"]

    session = await spawn_hpk_mock(functionality)

    response = await session.list_nodes("/a/b", flags=ListNodesFlags.ABSOLUTE)

    # propogates to functionality
    functionality.list_nodes.assert_called_once_with(
        "/a/b",
        flags=ListNodesFlags.ABSOLUTE,
    )

    # returns functionality response
    assert response == ["/a/b", "/a/c"]


@pytest.mark.asyncio()
async def test_list_nodes_info_propagates_to_functionality_and_back():
    functionality = Mock(spec=SessionMockFunctionality)
    functionality.list_nodes_info.return_value = {
        "/a/b": {"value": 1},
        "/a/c": {"value": 2},
    }

    session = await spawn_hpk_mock(functionality)

    response = await session.list_nodes_info(path="/a/b", flags=ListNodesFlags.ABSOLUTE)

    # propogates to functionality
    functionality.list_nodes_info.assert_called_once_with(
        path="/a/b",
        flags=ListNodesFlags.ABSOLUTE,
    )

    # returns functionality response
    assert response == {
        "/a/b": {"value": 1},
        "/a/c": {"value": 2},
    }


@pytest.mark.asyncio()
async def test_errors_in_set_functionality_are_transmitted_back():
    functionality = Mock(spec=SessionMockFunctionality)
    functionality.set.side_effect = Exception("Some error")

    session = await spawn_hpk_mock(functionality)

    with pytest.raises(Exception):  # noqa: B017
        await session.set(AnnotatedValue(path="/a/b", value=7))


@pytest.mark.asyncio()
async def test_errors_in_get_functionality_are_transmitted_back():
    functionality = Mock(spec=SessionMockFunctionality)
    functionality.get.side_effect = Exception("Some error")

    session = await spawn_hpk_mock(functionality)

    with pytest.raises(Exception):  # noqa: B017
        await session.get("/a/b")


@pytest.mark.asyncio()
async def test_errors_in_get_with_expression_functionality_are_transmitted_back():
    functionality = Mock(spec=SessionMockFunctionality)
    functionality.get_with_expression.side_effect = Exception("Some error")

    session = await spawn_hpk_mock(functionality)

    with pytest.raises(Exception):  # noqa: B017
        await session.get_with_expression("/a/b")


@pytest.mark.asyncio()
async def test_errors_in_set_with_expression_functionality_are_transmitted_back():
    functionality = Mock(spec=SessionMockFunctionality)
    functionality.set_with_expression.side_effect = Exception("Some error")

    session = await spawn_hpk_mock(functionality)

    with pytest.raises(Exception):  # noqa: B017
        await session.set_with_expression(AnnotatedValue(path="/a/b", value=7))


@pytest.mark.asyncio()
async def test_errors_in_subscribe_functionality_are_transmitted_back():
    functionality = Mock(spec=SessionMockFunctionality)
    functionality.subscribe_logic.side_effect = Exception("Some error")

    session = await spawn_hpk_mock(functionality)

    with pytest.raises(Exception):  # noqa: B017
        await session.subscribe("/a/b")


@pytest.mark.asyncio()
async def test_errors_in_list_nodes_functionality_are_transmitted_back():
    functionality = Mock(spec=SessionMockFunctionality)
    functionality.list_nodes.side_effect = Exception("Some error")

    session = await spawn_hpk_mock(functionality)

    with pytest.raises(Exception):  # noqa: B017
        await session.list_nodes("/a/b")


@pytest.mark.asyncio()
async def test_errors_in_list_nodes_info_functionality_are_transmitted_back():
    functionality = Mock(spec=SessionMockFunctionality)
    functionality.list_nodes_info.side_effect = Exception("Some error")

    session = await spawn_hpk_mock(functionality)

    with pytest.raises(Exception):  # noqa: B017
        await session.list_nodes_info(path="/a/b")
