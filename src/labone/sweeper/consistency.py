"""Defining checks for consistency of the sweeper settings.

The essence of this module is a list of consistency checks. They are all in the form
of a function, taking a sweeper and optionally throwing an error. This list may then
be used to check all consistency requirements of a sweeper.
"""


import typing as t

import numpy as np
from labone.core.helper import LabOneNodePath
from labone.sweeper.constants import _MAX_PLAYZERO_TIME, _MIN_SETTLING_TIME
from labone.sweeper.errors import SweeperConsistencyError
from labone.sweeper.local_session import sync_get


def _calculate_relative_error(value, granularity):
    relative_error = (value % granularity) / granularity

    # take into account values slightly beneath a full multiple of the granularity
    if relative_error > 0.5:
        relative_error = 1 - relative_error

    return relative_error


def is_within(
    path: LabOneNodePath,
    *,
    max: float | None = None,
    min: float | None = None,
    granularity: float | None = None,
    acceptable_relative_error: float = 0,
):
    def new_constrain(sweeper):
        value = sync_get(sweeper[path])
        if max is not None and value > max * (1 + acceptable_relative_error):
            raise SweeperConsistencyError(
                f"Value {value} of {path} is larger than {max}, which is invalid.",
            )

        if min is not None and value < min * (1 - acceptable_relative_error):
            raise SweeperConsistencyError(
                f"Value {value} of {path} is smaller than {min}, which is invalid.",
            )

        if (
            granularity is not None
            and _calculate_relative_error(value, granularity)
            > acceptable_relative_error
        ):
            raise SweeperConsistencyError(
                f"Value {value} of {path} is not a multiple of {granularity}.",
            )

    return new_constrain


def check(condition: t.Callable[["Sweeper"], bool], message: str):
    def new_constrain(sweeper):
        if not condition(sweeper):
            raise SweeperConsistencyError(message)

    return new_constrain


async def _check_device_type(sweeper):
    """Checks whether the provided device type is valid."""
    valid_device_types = ["SHFQA2", "SHFQA4", "SHFQC"]
    device = sync_get(sweeper.device)

    try:
        device_type = (await sweeper._device_tree[f"/{device}/features/devtype"]()).value
    except KeyError as e:
        msg = (
            f"Device {device} does not seem to be one of those types: "
            f"{', '.join(valid_device_types)}. "
            f"In fact, the type of the device could not be determined."
            f"Therefore, this device cannot be used with the Sweeper module."
        )
        raise SweeperConsistencyError(msg) from e

    if device_type not in valid_device_types:
        msg = (
            f"Device {device} is not one of those types: "
            f"{', '.join(valid_device_types)}. "
            f"Instead, it is of type {device_type} "
            f"and thus not supported in combination with the Sweeper module."
        )
        raise SweeperConsistencyError(msg)


async def _check_channel_index(sweeper):
    """Checks whether the provided channel index is valid

    Raises a ValueError exception if the checked setting was invalid.

    Arguments:
        channel_index: index of the qachannel to be checked
    """
    channel_index = sync_get(sweeper.rf.channel)
    device = sync_get(sweeper.device)
    device_type = await sweeper._device_tree[f"/{device}/features/devtype"]()

    if device_type == "SHFQA4":
        num_qa_channels = 4
    elif device_type == "SHFQA2":
        num_qa_channels = 2
    else:
        # SHFQC
        num_qa_channels = 1
    if channel_index >= num_qa_channels:
        raise SweeperConsistencyError(
            f"Device {device} only has a total of {num_qa_channels} QA channels."
            f"Thus channel {channel_index} is invalid.",
        )


def _check_integration_time(sweeper):
    max_int_len = ((2**23) - 1) * 4
    min_int_len = 4

    is_within(
        "/average/integration_time",
        min=min_int_len / sweeper._shf_sample_rate,
        max=max_int_len / sweeper._shf_sample_rate,
    )(sweeper)


def _check_envelope_waveform(sweeper):
    """Checks whether the suplied vector is a valid envelope waveform.

    Raises a ValueError exception if the checked setting was invalid.

    Arguments:
        wave_vector: the waveform vector to be checked
    """
    if not sync_get(sweeper.envelope.enable):
        return  # no checks if option is disabled

    wave_vector = sync_get(sweeper.envelope.waveform)

    if wave_vector is None:
        raise SweeperConsistencyError("No envelope waveform specified.")

    max_envelope_length = 2**16
    if len(wave_vector) > max_envelope_length:
        raise SweeperConsistencyError(
            f"Envelope length exceeds maximum of {max_envelope_length} samples.",
        )

    # Note: here, we check that the envelope vector elements are within the unit
    #       circle. This check is repeated by the envelope/wave node but it is
    #       stated here explicitly as a guidance to the user.
    if np.any(np.abs(wave_vector) > 1.0):
        raise SweeperConsistencyError(
            "The absolute value of each envelope vector element must be smaller "
            "than 1.",
        )


consistency_constrains = [
    check(
        lambda sweeper: sweeper.device is not None,
        "No device specified. Before running a sweep, a device must be set.",
    ),
    _check_device_type,
    _check_channel_index,
    is_within(
        "/rf/center_freq",
        min=0,
        max=8e9,
        granularity=100e6,
        acceptable_relative_error=0.01,
    ),
    is_within(
        "/rf/input_range",
        max=10,
        min=-50,
        granularity=5,
        acceptable_relative_error=0.001,
    ),
    is_within(
        "/rf/output_range",
        max=10,
        min=-30,
        granularity=5,
        acceptable_relative_error=0.001,
    ),
    is_within("/sweep/start_freq", min=-1e9),
    is_within("/sweep/stop_freq", max=1e9),
    check(
        lambda sweeper: sync_get(sweeper.sweep.start_freq)
        <= sync_get(sweeper.sweep.stop_freq),
        "Stop frequency must be larger than start_freq frequency.",
    ),
    is_within("/sweep/oscillator_gain", min=0, max=1),
    is_within("/sweep/settling_time", min=_MIN_SETTLING_TIME, max=_MAX_PLAYZERO_TIME),
    is_within("/sweep/wait_after_integration", min=0, max=_MAX_PLAYZERO_TIME),
    _check_integration_time,
    is_within(
        "/average/integration_delay",
        min=0,
        max=131e-6,
        granularity=2e-9,
        acceptable_relative_error=0.001,
    ),
    _check_envelope_waveform,
    is_within(
        "/envelope/delay",
        min=0,
        max=131e-6,
        granularity=2e-9,
        acceptable_relative_error=0.001,
    ),
]
