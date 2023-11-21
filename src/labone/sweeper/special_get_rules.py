"""Defining getting-behavior of local session.

Some paths of the sweeper nodetree require special behavior when getting the value.
The rules are defined in this module by giving a dictionary of {path: function} pairs.
The functions all take the sweeper as the only argument.
If a path does not occur in this dictionary, the default behavior of storing and
loading the path is used.
"""
from labone.sweeper.helper import _round_for_playzero
from labone.sweeper.local_session import sync_get


def actual_settling_time(sweeper) -> float:
    """Wait time between setting new frequency and triggering of integration.

    Note: the granularity of this time is 16 samples (8 ns).
    """
    return _round_for_playzero(
        sync_get(sweeper.sweep.settling_time),
        sample_rate=sweeper._shf_sample_rate,
    )


def actual_hold_off_time(sweeper) -> float:
    """Wait time after triggering the integration unit until the next cycle.

    Note: the granularity of this time is 16 samples (8 ns).
    """
    # ensure safe hold-off time for the integration results to be written to the external RAM.
    min_hold_off_time = 1032e-9

    return _round_for_playzero(
        max(
            min_hold_off_time,
            sync_get(sweeper.average.integration_delay)
            + sync_get(sweeper.average.integration_time)
            + sync_get(sweeper.sweep.wait_after_integration),
        ),
        sample_rate=sweeper._shf_sample_rate,
    )


def predicted_cycle_time(sweeper) -> float:
    """Predicted duration of each cycle of the spectroscopy loop.

    Note: this property only applies in self-triggered mode, which is active
    when the trigger source is set to None and `use_sequencer` is True.
    """
    return sync_get(sweeper.actual_settling_time) + sync_get(
        sweeper.actual_hold_off_time,
    )


special_get_rules = {
    "/actual_settling_time": actual_settling_time,
    "/actual_hold_off_time": actual_hold_off_time,
    "/predicted_cycle_time": predicted_cycle_time,
}
