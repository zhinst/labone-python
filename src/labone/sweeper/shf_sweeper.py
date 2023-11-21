import asyncio
import inspect
import json
import textwrap
import typing as t
from functools import partial
from pathlib import Path

from labone.core.helper import LabOneNodePath
from labone.core.value import Value
from labone.nodetree import construct_nodetree
from labone.nodetree.helper import join_path
from labone.nodetree.node import Node, PartialNode
from labone.sweeper.consistency import consistency_constrains
from labone.sweeper.constants import _SHF_SAMPLE_RATE
from labone.sweeper.device_transactions import DONT_SEND, configure_start
from labone.sweeper.helper import (
    AverageCyclic,
    load_sequencer_program,
)
from labone.sweeper.initial_configuration import initial_configuration
from labone.sweeper.local_session import LocalSession, sync_get, sync_set
from labone.sweeper.special_get_rules import special_get_rules

# TODO: typo in sweeper_nodes.json /average/num_averages Description: copy past error of previous node


class Sweeper(PartialNode):
    _shf_sample_rate = _SHF_SAMPLE_RATE

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
            Path.open(Path(__file__).parent / "sweeper_nodes.json").read(),
        )
        device_tree = await construct_nodetree(
            session=session,
            hide_kernel_prefix=False,
            use_enum_parser=True,
        )
        model_node = await construct_nodetree(
            session=LocalSession(path_to_info),
            hide_kernel_prefix=False,
            use_enum_parser=False,
        )
        instance = cls(device_tree=device_tree, model_node=model_node)

        instance.tree_manager.session.special_get_rules = {
            path: partial(rule, sweeper=instance)
            for path, rule in special_get_rules.items()
        }

        # setting the default values
        for path, initial_value in initial_configuration.items():
            sync_set(instance[path], initial_value)

        return instance

    async def run(self):
        await self.check_consistency(consistency_constrains)
        sequencer_program = self._generate_sequencer_program()
        num_results = sync_get(self.sweep.num_points) * sync_get(
            self.average.num_averages,
        )

        # await self.send_settings(configure_start)
        # await load_sequencer_program(
        #     self, sync_get(self.device), sync_get(self.rf.channel), sequencer_program,
        # )

        await asyncio.gather(
            self.send_settings(configure_start),
            #self.send_settings(self.result_length_and_averages_sequencer_settings),
            #self.send_settings(self.activate_sweep_settings),
            load_sequencer_program(
                self, sync_get(self.device), sync_get(self.rf.channel), sequencer_program
            ),
        )
        # todo: wo genau ist die begin option und ist es sicher dass sie zuletzt gesetzt wird?

        hundred_milliseconds = 0.1
        await asyncio.sleep(hundred_milliseconds)
        # todo: wirklich erst nach einer Weile subscriben? Vllt sind dann schon ergebnisse nicht mehr da??

        data_queue = await self._prefix_node.spectroscopy.result.data.wave.subscribe()
        for i in range(num_results):
            yield await data_queue.get()

        data_queue.disconnect()

    async def send_settings(
        self,
        settings: dict[
            LabOneNodePath,
            LabOneNodePath | t.Callable[["Sweeper"], Value | DONT_SEND],
        ],
    ) -> None:
        """Send settings to the device.

        Args:
            settings: A dictionary of destination node of device to source. The source
                can either be a node path of the sweeper nodetree or a function that
                returns the value to be set.
        """
        promises = []
        for destination, source in settings.items():
            if callable(source):
                value = source(self)
            else:
                value = sync_get(self[source])

            if value != DONT_SEND:
                destination_node = self._prefix_node[destination]
                promises.append(destination_node(value))

        await asyncio.gather(*promises)

    async def check_consistency(self, constrains):
        """raises:
        SweeperConsistencyError: If any constrain is violated.
        """
        for constrain in constrains:
            if inspect.iscoroutinefunction(constrain):
                await constrain(self)  # guarantee order even if async
            else:
                constrain(self)

    def _generate_sequencer_program(self):
        """Internal method, which generates the SeqC code for a sweep
        """
        seqc_header = textwrap.dedent(
            f"""
            const OSC0 = 0;
            setTrigger(0);
            configFreqSweep(OSC0, {sync_get(self.sweep.start_freq)}, {self._freq_step});
            """,
        )

        seqc_wait_for_trigger = (
            "waitDigTrigger(1);"
            if sync_get(self.trigger.source) is not None
            else "// self-triggering mode"
        )

        seqc_loop_body = textwrap.dedent(
            f"""
            {seqc_wait_for_trigger}

            // define time from setting the oscillator frequency to sending
            // the spectroscopy trigger
            playZero({self._get_playzero_settling_samples()});

            // set the oscillator frequency depending on the loop variable i
            setSweepStep(OSC0, i);
            resetOscPhase();

            // define time to the next iteration
            playZero({self._get_playzero_hold_off_samples()});

            // trigger the integration unit and pulsed playback in pulsed mode
            setTrigger(1);
            setTrigger(0);
            """,
        )

        averaging_loop_arguments = (
            f"var j = 0; j < {sync_get(self.average.num_averages)}; j++"
        )
        sweep_loop_arguments = f"var i = 0; i < {sync_get(self.sweep.num_points)}; i++"

        if sync_get(self.average.mode) == AverageCyclic:
            outer_loop_arguments = averaging_loop_arguments
            inner_loop_arguments = sweep_loop_arguments
        else:
            outer_loop_arguments = sweep_loop_arguments
            inner_loop_arguments = averaging_loop_arguments

        seqc = (
            seqc_header
            + textwrap.dedent(
                f"""
                for({outer_loop_arguments}) {{
                    for({inner_loop_arguments}) {{""",
            )
            + textwrap.indent(seqc_loop_body, " " * 8)
            + textwrap.dedent(
                """
                    }
                }
                """,
            )
        )

        return seqc

    @property
    def _prefix_node(self) -> Node:
        return self._device_tree[
            join_path(
                (sync_get(self.device), "qachannels", str(sync_get(self.rf.channel))),
            )
        ]

    @property
    def _freq_step(self) -> float:
        """Returns the frequency step size according to the sweep settings
        """
        return (sync_get(self.sweep.stop_freq) - sync_get(self.sweep.start_freq)) / (
            sync_get(self.sweep.num_points) - 1
        )

    def _get_playzero_settling_samples(self) -> int:
        """Returns an integer number of samples corresponding to the settling time
        The return value respects the granularity of the playZero SeqC command.

        Returns:
            the number of samples corresponding to the settling time
        """
        return round(sync_get(self.actual_settling_time) * self._shf_sample_rate)

    def _get_playzero_hold_off_samples(self) -> int:
        """Returns the hold-off time needed per iteration of the the inner-most
        loop of the SeqC program. The return value respects the minimal hold-off time
        and the granularity of the playZero SeqC command.

        Returns:
            the number of samples corresponding to the hold-off time
        """
        return round(sync_get(self.actual_hold_off_time) * self._shf_sample_rate)
