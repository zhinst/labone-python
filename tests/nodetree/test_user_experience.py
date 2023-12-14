"""
These tests do not test required behavior of the module.
Thus, they may fail even if everything is working correctly.
They are in place to make sure the user experience is as good as possible.
If new features are implemented or context changes,
the tests may not be relevant any more.
That is why for every test, there is a description to explain its purpose.
By consulting the description, it will the possible to determine
if the test is still relevant.
"""

from __future__ import annotations

import pytest

from tests.mock_server_for_testing import get_unittest_mocked_node


def check_message_useful(message: str, keywords: list[str]) -> bool:
    """Checks that an (error) message is suitable for a given situation.

    This function is a bare trial to assess the usefulness of
    an error message to the user. It is a crude heuristic.
    Human judgement is still the source of truth.

    If this function misclassifies an error message as not useful,
    some keywords can be adjusted to make it succeed.
    """
    return sum([k in message.lower() for k in keywords]) / len(keywords) >= 0.5


class UsefulErrorMessage:
    def __init__(self, keywords: list[str]):
        self.keywords = keywords

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            if not check_message_useful(str(exc_value), self.keywords):
                msg = f"Error message does not seem to be useful: {exc_value}"
                raise AssertionError(msg)
            return True

        msg = "No error was raised - even tough it was inticipated."
        raise AssertionError(msg)


class TestOnlyLeafNodesCanBeSubscribedTo:
    """Only leaf nodes can be subscribed to,
    but it is not always obvious which nodes are leaf nodes.
    Therefore, calling subscribe on a non-leaf node can happen
    accidentely.
    """

    @pytest.mark.asyncio()
    async def test_subscribe_to_partial_node_raises(self):
        node = await get_unittest_mocked_node({"/a/b": {}})

        with UsefulErrorMessage(["partial", "cannot", "subscri", "leaf"]):
            await node.a.subscribe()

    @pytest.mark.asyncio()
    async def test_subscribe_to_wildcard_node_raises(self):
        node = await get_unittest_mocked_node({"/a/b": {}})

        with UsefulErrorMessage(["wildcard", "cannot", "subscri", "leaf"]):
            await node["*"].b.subscribe()


class TestWildcardNodeDoesNotKnowTreeStructure:
    """Once wildcards are used in paths, it becomes harder
    to determine which subnodes exist. Computing this is
    not done for performance reasons. For this reason,
    structure related operations are not supported on
    wildcard nodes. Instead of giving confusing results,
    an error message is shown.
    """

    @pytest.mark.asyncio()
    async def test_wildcard_node_contains_raises(self):
        node = await get_unittest_mocked_node({"/a/b": {}})

        with UsefulErrorMessage(["wildcard", "cannot", "contain"]):
            "/a/b" in node["*"]  # noqa: B015

    @pytest.mark.asyncio()
    async def test_wildcard_node_iter_raises(self):
        node = await get_unittest_mocked_node({"/a/b": {}})

        with UsefulErrorMessage(["wildcard", "cannot", "iter"]):
            for _ in node["*"]:
                pass

    @pytest.mark.asyncio()
    async def test_wildcard_node_len_raises(self):
        node = await get_unittest_mocked_node({"/a/b": {}})

        with UsefulErrorMessage(["wildcard", "cannot", "len"]):
            len(node["*"])


@pytest.mark.asyncio()
async def test_calling_result_node_raises():
    """
    Result nodes are already call-results.
    Therefore there is no semantic in calling them.
    May happen accidentely though.
    """
    node = await get_unittest_mocked_node({"/a": {}})
    result = await node()

    with UsefulErrorMessage(["result", "cannot", "get", "set"]):
        await result()


@pytest.mark.asyncio()
async def test_using_wildcards_in_result_node_raises():
    """
    Wildcards are useful for specifying which paths to get/set.
    In result nodes, there is no point in using them and it is
    not supported. However, this is not obvious.
    """
    node = await get_unittest_mocked_node({"/a": {}})
    result = await node()

    with UsefulErrorMessage(["result", "cannot", "wildcard"]):
        await result["*"]


@pytest.mark.asyncio()
async def test_wait_for_state_change_on_partial_node_raises():
    """
    Waiting for a change is done via providing a value.
    Waiting for multiple nodes only makes sense if they
    are all of the same type. This is most often not
    the case for partial nodes. May be tried accidentely though.
    """
    node = await get_unittest_mocked_node({"/a/b": {}})

    with UsefulErrorMessage(["partial", "cannot", "state"]):
        await node.a.wait_for_state_change(5)


@pytest.mark.asyncio()
async def test_dir_shows_subpaths():
    """
    In order to make working on the nodetree easy, it is
    important to know what path extensions are possible.
    So the __dir__ method is supposed to show the subpaths.
    """
    node = await get_unittest_mocked_node({"/a/b": {}, "/a/c": {}})

    assert "b" in dir(node.a)
    assert "c" in dir(node.a)


@pytest.mark.asyncio()
async def test_dir_shows_subpaths_keywords_with_underscore():
    """
    Some path segments collide with python keywords.
    Therefor, it wont be possible to type node.in.debug
    Thats why the __dir__ method is supposed to show keywords
    with an underscore, which is the intended way to use this in python.
    """
    node = await get_unittest_mocked_node({"/in": {}})
    assert "in_" in dir(node)
