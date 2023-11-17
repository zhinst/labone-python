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
                f"Channel must be one of {', '.join(cls._valid_channels)} whereas {channel} was given."
            )
        if input not in cls._valid_inputs:
            raise SweeperSettingError(
                f"Input must be one of {', '.join(cls._valid_inputs)} whereas {input} was given."
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


if __name__ == "__main__":
    trigger = ChannelTrigger(channel=3, input=1)
    trigger2 = ChannelTrigger(channel=3, input=1)
    assert trigger2 == trigger
    print(trigger.as_string())
