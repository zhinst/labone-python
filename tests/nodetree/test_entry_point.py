import asyncio
from unittest.mock import ANY, Mock, patch

import pytest
from labone.nodetree import construct_nodetree


def _get_future(value):
    future = asyncio.Future()
    future.set_result(value)
    return future


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ("use_enum_parser", "have_custom_enum_parser", "expected_parsing_result"),
    [
        (False, False, "t"),
        (False, True, "tt"),
        (True, False, "ttt"),
        (True, True, "tttttt"),
    ],
)
async def test_construct_nodetree_custom_parser(
    use_enum_parser,
    have_custom_enum_parser,
    expected_parsing_result,
):
    custom_enum_parser = (lambda x: x + x) if have_custom_enum_parser else None

    list_node_info = {}
    session_mock = Mock()
    session_mock.list_nodes_info = Mock(return_value=_get_future(list_node_info))

    nodetree_mock = Mock()
    nodetree_mock._root_prefix = ()
    nodetree_mock.root = "result"

    with patch(
        "labone.nodetree.entry_point.NodeTreeManager",
        return_value=nodetree_mock,
        autospec=True,
    ) as new_mock, patch(
        "labone.nodetree.entry_point.get_default_enum_parser",
        autospec=True,
        return_value=lambda x: x + x + x,
    ) as enum_mock:
        tree = await construct_nodetree(
            session=session_mock,
            hide_kernel_prefix="hide_kernel_prefix",
            use_enum_parser=use_enum_parser,
            custom_parser=custom_enum_parser,
        )

    session_mock.list_nodes_info.assert_called_once_with("*")
    if use_enum_parser:
        enum_mock.assert_called_once_with(list_node_info)

    new_mock.assert_called_once_with(
        session=session_mock,
        path_to_info=list_node_info,
        parser=ANY,
        hide_kernel_prefix="hide_kernel_prefix",
    )
    assert tree == "result"
    received_parser = new_mock.call_args_list[0][1]["parser"]

    assert received_parser("t") == expected_parsing_result
