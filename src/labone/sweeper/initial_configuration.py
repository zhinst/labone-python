"""Defining the start configuration of a sweeper.

The configuration is given as a mapping of a sweeper-path to its initial value.
For more information on the effects of each setting, see [todo].
"""
from labone.sweeper.constants import _MIN_SETTLING_TIME
from labone.sweeper.helper import AverageCyclic, NoTrigger

initial_configuration = {
    "/device": None,  # specfied by the user
    "/sweep/start_freq": -300e6,
    "/sweep/stop_freq": 300e6,
    "/sweep/num_points": 100,
    "/sweep/mapping": None,  # deprecated
    "/sweep/oscillator_gain": 1,
    "/sweep/settling_time": _MIN_SETTLING_TIME,
    "/sweep/wait_after_integration": 0.0,
    "/sweep/mode": 1,  # deprecated (always use sequencer mode)
    "/sweep/psd": False,
    "/rf/channel": 0,
    "/rf/input_range": -5,
    "/rf/output_range": 0,
    "/rf/center_freq": 5e9,
    "/average/integration_time": 1e-3,
    "/average/num_averages": 1,
    "/average/mode": AverageCyclic,
    "/average/integration_delay": 224.0e-9,
    "/trigger/source": NoTrigger,
    "/trigger/level": 0.5,
    "/trigger/input_impedance": 1,  # option corresponding to 50 Ohm
    "/trigger/sw_trigger_mode": None,  # deprecated
    "/envelope/enable": False,
    "/envelope/waveform": None,  # needs to be specified by the user
    "/envelope/delay": 0.0,
}
