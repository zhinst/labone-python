from labone.instrument import Instrument

from tests.test_dataserver import MockModelNode


class MockInstrument(Instrument):
    def __init__(self):
        super().__init__(
            serial="serial",
            model_node=MockModelNode(),
        )


def test_repr():
    serial = "dev1234"
    mock_instrument = MockInstrument()
    mock_instrument._serial = serial
    assert serial in repr(mock_instrument)
