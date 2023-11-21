"""Defining how to send settings to the device.

Several setting-packages are defined here. A package is a dictionary of the form
{destination: source}. The destination is a node path of the device, the source is
either a node path of the sweeper nodetree or a function that returns the value to be
set. The source may also be DONT_SEND, in which case the setting will not be sent.
"""

from labone.sweeper.helper import NoTrigger
from labone.sweeper.local_session import sync_get

DONT_SEND = object()


configure_rf_frontends = {
    "/input/range": "/rf/input_range",
    "/output/range": "/rf/output_range",
    "/centerfreq": "/rf/center_freq",
    "/oscs/0/gain": "/sweep/oscillator_gain",
    "/mode": lambda _: "spectroscopy",
}

configure_envelope = {
    "/spectroscopy/envelope/wave": lambda sweeper: sync_get(
        sweeper.envelope.waveform,
    ).astype("complex128")
    if sweeper.envelope.enable
    else DONT_SEND,
    "/spectroscopy/envelope/enable": lambda sweeper: bool(
        sync_get(sweeper.envelope.enable),
    ),
    "/spectroscopy/envelope/delay": lambda sweeper: sync_get(sweeper.envelope.delay)
    if sync_get(sweeper.envelope.enable)
    else DONT_SEND,
}

configure_spectroscopy_delay = {
    "/spectroscopy/delay": lambda sweeper: sync_get(sweeper.average.integration_delay)
    if sync_get(sweeper.average.num_averages) != 1
    else DONT_SEND,
}

configure_integration_time = {
    "/spectroscopy/length": lambda sweeper: round(
        sync_get(sweeper.average.integration_time) * sweeper._shf_sample_rate,
    ),
}


configure_psd = {
    "/spectroscopy/psd/enable": lambda sweeper: bool(sync_get(sweeper.sweep.psd)),
}


configure_triggering_via_sequencer = {
    "/generator/auxtriggers/0/channel": lambda sweeper: sync_get(
        sweeper.trigger.source,
    ).as_string()
    if sync_get(sweeper.trigger.source) != NoTrigger
    else DONT_SEND,
    "/spectroscopy/trigger/channel": lambda sweeper: f"chan{sync_get(sweeper.rf.channel)}seqtrig0"
    if sync_get(sweeper.trigger.source) == NoTrigger
    else DONT_SEND,  # TODO not finished yet
}

# def conf_tr(sweeper):
#     if sync_get(sweeper.trigger.source) != NoTrigger:
#         yield "generator/auxtriggers/0/channel", sync_get(sweeper.trigger.source).as_string()
#     else:
#         yield "spectroscopy/trigger/channel", f"chan{sync_get(sweeper.rf.channel)}seqtrig0"


# # TODO refactor
# result_length_and_averages_sequencer_settings = {
#     ("spectroscopy", "result", "mode"): ("avg", "mode"),
#     ("spectroscopy", "result", "length"): ("sweep", "num_points"),
#     ("spectroscopy", "result", "averages"): ("sweep", "averages"),
# }
#
# activate_sweep_settings = {
#     ("spectroscopy", "result", "enable"): lambda _: 1,
#     ("generator", "single"): lambda _: 1,
#     ("generator", "enable"): lambda _: 1,
# }

configure_start = (
    configure_rf_frontends
    | configure_envelope
    | configure_spectroscopy_delay
    | configure_integration_time
    | configure_psd
)
