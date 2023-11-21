import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cache

import numpy as np
from labone.sweeper.errors import SweeperSettingError


class AveragingMode(ABC):
    @staticmethod
    @abstractmethod
    def get_freq_sequencer(sweeper, num_acquired: int):
        ...

    @staticmethod
    @abstractmethod
    def average_samples(self, vec):
        ...


class AverageCyclic(AveragingMode):
    @staticmethod
    def get_freq_sequencer(sweeper, num_acquired: int) -> float:
        return sweeper._sweep.start_freq + sweeper._freq_step * (
            (num_acquired - 1) % sweeper._sweep.num_points
        )

    @staticmethod
    def average_samples(self, vec):
        avg_vec = np.zeros(self._sweep.num_points, dtype="complex")

        total_measurements = self._sweep.num_points * self._avg.num_averages
        for i in range(self._sweep.num_points):
            avg_range = range(i, total_measurements, self._sweep.num_points)
            avg_vec[i] = np.mean(vec[avg_range])

        return avg_vec


class AverageSequential(AveragingMode):
    @staticmethod
    def get_freq_sequencer(sweeper, num_acquired: int):
        return sweeper._sweep.start_freq + sweeper._freq_step * (
            (num_acquired - 1) // sweeper._avg.num_averages
        )

    @staticmethod
    def average_samples(self, vec):
        avg_vec = np.zeros(self._sweep.num_points, dtype="complex")

        for i in range(self._sweep.num_points):
            start_ind = i * self._avg.num_averages
            avg_range = range(start_ind, start_ind + self._avg.num_averages)
            avg_vec[i] = np.mean(vec[avg_range])

        return avg_vec


class TriggerSource(ABC):
    ...


class NoTrigger(TriggerSource):
    ...


class ChannelTrigger(TriggerSource):
    _channel: int
    _input: int

    _valid_channels = (1, 2, 3, 4)
    _valid_inputs = (1, 2)

    def __new__(cls, *, channel: int, input: int):
        if channel not in cls._valid_channels:
            raise SweeperSettingError(
                f"Channel must be one of {', '.join(cls._valid_channels)} whereas {channel} was given.",
            )
        if input not in cls._valid_inputs:
            raise SweeperSettingError(
                f"Input must be one of {', '.join(cls._valid_inputs)} whereas {input} was given.",
            )
        return cls._build_new_subclass(channel=channel, input=input)

    @cache
    @staticmethod
    def _build_new_subclass(*, channel: int, input: int):
        class SpecificChannelTrigger(ChannelTrigger):
            _channel = channel
            _input = input

        return SpecificChannelTrigger

    @classmethod
    def as_string(cls):
        return f"channel{cls._channel}_trigger_input{cls._input}"

    @classmethod
    @property
    def channel(cls):
        return cls._channel

    @classmethod
    @property
    def input(cls):
        return cls._input


@dataclass
class AvgConfig:
    """Averaging settings for a sweep"""

    integration_time: float = 1e-3  #: total time while samples are integrated
    num_averages: int = 1  #: times to measure each frequency point
    mode: AveragingMode = AverageCyclic
    """averaging mode, which can be "cyclic", to first scan the frequency and then
    repeat, or "sequential", to average each point before changing the frequency"""
    integration_delay: float = 224.0e-9
    """time delay after the trigger for the integrator to start"""


def _round_for_playzero(time_interval: float, sample_rate: float):
    """Rounds a time interval to the granularity of the playZero SeqC command

    Arguments:
        time_interval: the time interval to be rounded for the playZero command
        sample_rate:    the sample rate of the instrument

    Returns:
        rounded the time interval
    """
    playzero_granularity = 16

    # round up the number of samples to multiples of playzero_granularity
    num_samples = (
        (round(time_interval * sample_rate) + (playzero_granularity - 1))
        // playzero_granularity
    ) * playzero_granularity
    return num_samples / sample_rate


async def load_sequencer_program(
    sweeper,
    device_id: str,
    channel_index: int,
    sequencer_program: str,
    **_,
) -> None:
    """Compiles and loads a program to a specified sequencer.

    This function is composed of 4 steps:
        1. Reset the generator to ensure a clean state.
        2. Compile the sequencer program with the offline compiler.
        3. Upload the compiled binary elf file.
        4. Validate that the upload was successful and the generator is ready
           again.

    Args:
        daq: Instance of a Zurich Instruments API session connected to a Data
            Server. The device with identifier device_id is assumed to already
            be connected to this instance.
        device_id: SHFQA device identifier, e.g. `dev12004` or 'shf-dev12004'.
        channel_index: Index specifying to which sequencer the program below is
            uploaded - there is one sequencer per channel.
        sequencer_program: Sequencer program to be uploaded.

    Raises:
        RuntimeError: If the Upload was not successfully or the device could not
            process the sequencer program.
    """
    # Start by resetting the sequencer.
    # Compile the sequencer program.

    _, device_type, device_options = await asyncio.gather(
        sweeper._device_tree[f"/{device_id}/qachannels/{channel_index}/generator/reset"](1),
        sweeper._device_tree[f"/{device_id}/features/devtype"](),
        sweeper._device_tree[f"/{device_id}/features/options"](),
    )

    elf, _ = compile_seqc(
        sequencer_program, device_type, device_options, channel_index, sequencer="qa",
    )
    # Upload the binary elf file to the device.

    await sweeper._device_tree[f"/{device_id}/qachannels/{channel_index}/generator/elf/data"](elf)  # TODO: changes from 'upload' to 'data', correct?
    # Validate that the upload was successful and the generator is ready again.

    if not await sweeper._device_tree[f"/{device_id}/qachannels/{channel_index}/generator/ready"]():
        raise RuntimeError(
            "The device did not not switch to into the ready state after the upload.",
        )


def compile_seqc(*_, **__):
    # TODO
    return "Dummy compiled seqc programm.", None


if __name__ == "__main__":
    trigger = ChannelTrigger(channel=3, input=1)
    trigger2 = ChannelTrigger(channel=3, input=1)
    assert trigger2 == trigger
    print(trigger.as_string())
