import json
from pathlib import Path

import pytest

from labone.sweeper.local_session import LocalSession
from labone.sweeper.shf_sweeper import Sweeper
from tests.nodetree.conftest import zi_structure, StructureProvider


@pytest.mark.asyncio()
async def test_1():
    session = LocalSession(
        StructureProvider(
            json.load(
                Path.open(
                    Path(__file__).parent.parent
                    / "nodetree"
                    / "resources"
                    / "zi_nodes_info.json"
                )
            )
        ).nodes_to_info
    )
    sweeper = await Sweeper.create(session=session)
    assert sweeper


@pytest.mark.asyncio()
async def test_2():
    session = LocalSession(
        StructureProvider(
            json.load(
                Path.open(
                    Path(__file__).parent.parent
                    / "nodetree"
                    / "resources"
                    / "zi_nodes_info.json"
                )
            )
        ).nodes_to_info
    )
    sweeper = await Sweeper.create(session=session)
    await sweeper.device('dev1234_object')

    await sweeper.rf.input_range(5)



    async for r in sweeper.run():
        print(r)