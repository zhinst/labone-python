import asyncio
import json
from pathlib import Path

import pytest
from labone.core import AnnotatedValue
from labone.sweeper.local_session import LocalSession, sync_set
from labone.sweeper.shf_sweeper import Sweeper

from tests.nodetree.conftest import StructureProvider


@pytest.mark.asyncio()
async def test_1():
    session = LocalSession(
        StructureProvider(
            json.load(
                Path.open(
                    Path(__file__).parent.parent
                    / "nodetree"
                    / "resources"
                    / "zi_nodes_info.json",
                ),
            ),
        ).nodes_to_info,
    )
    sweeper = await Sweeper.create(session=session)
    sync_set(sweeper.rf.input_range, 5)

    # sweeper.rf.input_range = 5
    # sweeper.rf.input_range << sync(5)
    # sweeper.rf.input_range = 5

    # print( ~sweeper.rf.input_range)

    assert sweeper

class MockQueue(asyncio.Queue):
    def __init__(self, data):
        super().__init__()
        self.data = data

    def get(self):
        return self.data.pop(0)

    def disconnect(self):
        pass


def _get_future(value):
    future = asyncio.Future()
    future.set_result(value)
    return future


@pytest.mark.asyncio()
async def test_2():
    session = LocalSession(
        StructureProvider(
            json.load(
                Path.open(
                    Path(__file__).parent.parent
                    / "nodetree"
                    / "resources"
                    / "shfqa_nodes_info.json",
                ),
            ),
        ).nodes_to_info,
    )

    session._memory["/dev12000/features/devtype"] = AnnotatedValue(path="/dev12000/features/devtype", value="SHFQA4")

    session.subscribe = lambda *args, **kwargs: _get_future(
        MockQueue([
            _get_future(AnnotatedValue(value=i, path="/some/path")) for i in range(10000)]),
    )

    sweeper = await Sweeper.create(session=session)
    sync_set(sweeper.device, "dev12000")

    # async for r in sweeper.run():
    #     print(r.value)

    result_list = [value async for value in sweeper.run()]
    print(result_list)
    # print(
    #     list(
    #         await asyncio.gather(*(value async for value in sweeper.run())))
    # )


# def normal_workflow():
#     session = KernelSession()
#     sweeper = Sweeper.create_for_device(device)
