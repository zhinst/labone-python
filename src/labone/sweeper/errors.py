from labone.errors import LabOneError


class SweeperError(LabOneError):
    """Base class for all errors raised by the Sweeper module."""



class SweeperSettingError(SweeperError):
    """Raised when a setting is not supported by the Sweeper module."""



class SweeperConsistencyError(SweeperError):
    """Raised when the configuration of the sweeper is found to be invalid."""



class SweeperLocalStateError(SweeperError):
    """Raised when the interaction with the local session is used in a wrong way."""

