import asyncio
import json
from pathlib import Path

from labone.core.value import Value
from labone.nodetree import construct_nodetree
from labone.nodetree.helper import NormalizedPathSegment, join_path
from labone.nodetree.node import Node, NodeTreeManager, PartialNode
from labone.sweeper.local_session import LocalSession

import typing as t

from labone.sweeper.sweep_result_queue import SweepResultQueue


class Sweeper(PartialNode):

    # node on device <- rule how to set it (may be a function or a sweeper path)
    start_sweep_settings = {
        ("input", "range"): ("rf", "input_range"),
        ("output", "range"): ("rf", "output_range"),
        ("centerfreq",): ("rf", "center_freq"),
        ("osc", 0, "gain"): ("sweep", "oscillator_gain"),
        ("mode",): ("spectroscopy",)
        # todo continue
    }

    result_length_and_averages_sequencer_settings = {
        ("spectroscopy", "result", "mode"): ("avg", "mode"),
        ("spectroscopy", "result", "length"): ("sweep", "num_points"),
        ("spectroscopy", "result", "averages"): ("sweep", "averages"),
    }

    activate_sweep_settings = {
        ("spectroscopy", "result", "enable"): lambda _: 1,
        ("generator", "single"): lambda _: 1,
        ("generator", "enable"): lambda _: 1,
    }

    def __init__(self, *, device_tree, model_node: Node):
        super().__init__(
            tree_manager=model_node.tree_manager,
            path_segments=model_node.path_segments,
            subtree_paths=model_node.subtree_paths,
            path_aliases=model_node.path_aliases,
        )
        self._device_tree = device_tree

    @classmethod
    async def create(cls, session):
        path_to_info = json.loads(
            Path.open(Path(__file__).parent / "sweeper_nodes.json").read()
        )
        device_tree = await construct_nodetree(
            session=session,
            hide_kernel_prefix=True,
            use_enum_parser=True,
        )
        model_node = await construct_nodetree(
            session=LocalSession(path_to_info),
            hide_kernel_prefix=False,
            use_enum_parser=False,
        )
        instance = cls(device_tree=device_tree, model_node=model_node)

        # setting the default values
        await instance.sweep.start_freq(-300e6)
        await instance.sweep.stop_freq(300e6)
        # Todo continue

        return instance

    async def run(self):
        await asyncio.gather(
            self.send_settings(self.start_sweep_settings),
            self.send_settings(self.result_length_and_averages_sequencer_settings),
            self.send_settings(self.activate_sweep_settings),
        )

        num_results = await self.sweep.num_points() * await self._avg.num_averages()
        hundred_milliseconds = 0.1
        await asyncio.sleep(hundred_milliseconds)

        data_queue = await (await self._data_node).subscribe()
        for i in range(num_results):
            yield await data_queue.get()

        data_queue.disconnect()

    async def send_settings(
        self,
        settings: dict[
            tuple[NormalizedPathSegment, ...],
            tuple[NormalizedPathSegment, ...] | t.Callable[["Sweeper"], Value],
        ],
    ) -> None:
        """Send settings to the device.

        Args:
            settings: A dictionary of destination node of device to source. The source
                can either be a node path of the sweeper nodetree or a function that
                returns the value to be set.
        """
        promises = [
            (await self._prefix_node)[join_path(destination)](  # destination as node
                source(self)
                if callable(source)
                else await self[join_path(source)]()  # retrieve value from sweeper node
            )
            for destination, source in settings.items()
        ]

        await asyncio.gather(*promises)

    @property
    async def _prefix_node(self) -> Node:
        return self._device_tree[
            join_path((await self.device(), "qachannels", await self.rf.channel()))
        ]

    @property
    async def _acquired_node(self) -> Node:
        return (await self._prefix_node).spectroscopy.result.acquired

    @property
    async def _data_node(self) -> Node:
        return (await self._prefix_node).spectroscopy.result.data.wave
