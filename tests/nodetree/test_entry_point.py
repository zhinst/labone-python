import pytest
from labone.nodetree import Node, construct_nodetree

from tests.nodetree.conftest import session_mock


@pytest.mark.asyncio()
async def test_construct_nodetree(session_mock):
    tree = await construct_nodetree(
        session=session_mock,
        hide_kernel_prefix=True,
        use_enum_parser=True,
    )
    assert isinstance(tree, Node)


@pytest.mark.asyncio()
async def test_construct_nodetree_with_custom_parser(session_mock):
    tree = await construct_nodetree(
        session=session_mock,
        hide_kernel_prefix=True,
        use_enum_parser=False,
        custom_parser=lambda x: x,
    )
    assert isinstance(tree, Node)
