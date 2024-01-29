"""LabOne Error classes associated with the nodetree."""

from labone.errors import LabOneError


class LabOneNodetreeError(LabOneError):
    """Base class for all LabOne nodetree errors."""


class LabOneInvalidPathError(LabOneNodetreeError):
    """Raised when the path is invalid."""


class LabOneInappropriateNodeTypeError(LabOneNodetreeError):
    """Raised when a node is not of the expected type."""


class LabOneNoEnumError(LabOneNodetreeError):
    """Raised when it is tried to get a enum for a node which is not an enum."""


class LabOneNotImplementedError(LabOneNodetreeError, NotImplementedError):
    """Raised when a method is not implemented."""
