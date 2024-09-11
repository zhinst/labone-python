"""Module providing a parser for enumerated integer values.

Some integer values coming from the server are enumerated. This means that
only a limited set of integer values are valid and each integer value is
associated to a keyword. This module provides a parser that converts the
integer value to an Enum object.

The Enum object is created dynamically based on the node information. This
is not ideal since it makes it hard to use the Enum as a user. Nevertheless
its beneficial to apply the parser to the values coming from the server since
it makes the values more readable and easier to use. Since we return a IntEnum
object the values can be treated as integers and used as such.
"""

from __future__ import annotations

import logging
import typing as t
from enum import Enum, IntEnum
from functools import lru_cache

from labone.node_info import _parse_option_keywords_description

if t.TYPE_CHECKING:
    from labone.core import AnnotatedValue
    from labone.core.helper import LabOneNodePath
    from labone.core.session import NodeInfo as NodeInfoType

logger = logging.getLogger(__name__)

T = t.TypeVar("T")


class NodeEnumMeta:
    """Custom Metaclass for NodeEnum.

    Note: Required to enable pickling of a NodeEnum value.

    It simply servers the purpose to recreate a NodeEnum for a given enum
    value. Since the NodeEnums are created dynamically there is no way recreate
    a NodeEnum value since python can not find the definition. This class
    bypasses this problem by providing the functionality to recreate the
    Enum on the fly.

    Warning:
        Although the class of the resulting enum object looks and feels
        the same as the original one it is not. Therefore comparing the `type`
        will fail. This is however the only limitation.
        (type(value_old) != type(value_new) but value_old == value_new)

    Args:
        value: Value of the NodeEnum object that should be created.
        class_name: Name of the NodeEnum class.
        names: Mapping of the enum names to their corresponding integer value.
        module: Should be set to the module this class is being created in.
    """

    def __new__(  # type: ignore[misc] # noqa: D102
        cls,
        value: int,
        class_name: str,
        names: dict[str, int],
        module: str,
    ) -> Enum:
        new_enum = NodeEnum(class_name, names, module=module)
        return new_enum(value)  # type: ignore[func-returns-value]


class NodeEnum(IntEnum):
    """Custom dynamically picklable IntEnum class.

    The Enum values for a device are created dynamically in toolkit based on
    the node information. Since they are not predefined but rather created
    dynamically, the are not picklable. This custom child class of IntEnum
    overwrites the reduce function that returns all information required to
    recreate the Enum class in `NodeEnumMeta`.

    For more information on the reduce functionality and how it is used within
    the pickle package see
    [pep307](https://peps.python.org/pep-0307/#extended-reduce-api).
    """

    # Required for typing
    def __init__(self, *args, **kwargs) -> None: ...

    # Required for typing
    def __call__(self, *args, **kwargs) -> None:  # noqa: D102
        ...

    def __reduce_ex__(
        self,
        _: object,
    ) -> tuple[type[NodeEnumMeta], tuple[int, str, dict, str]]:
        return NodeEnumMeta, (
            self._value_,
            self.__class__.__name__,
            {key: int(value) for key, value in self.__class__._member_map_.items()},  # type: ignore[call-overload]
            self.__class__.__module__,
        )


def _get_enum(*, info: NodeInfoType, path: LabOneNodePath) -> NodeEnum | None:
    """Enum of the node options."""
    if "Options" not in info:
        return None

    keyword_to_option = {}
    for key, option_string in info["Options"].items():
        keywords, _ = _parse_option_keywords_description(option_string)
        for keywork in keywords:
            keyword_to_option[keywork] = int(key)

    return NodeEnum(path, keyword_to_option, module=__name__)


def get_default_enum_parser(
    path_to_info: dict[LabOneNodePath, NodeInfoType],
) -> t.Callable[[AnnotatedValue], AnnotatedValue]:
    """Get a generic parser for enumerated integer values.

    The returned parser can be called with an annotated value. If the value
    is enumerated and the corresponding Enum can be found, the value will be
    converted to the Enum.

    The lookup is cached to speed up the process.

    Args:
        path_to_info: Mapping of node paths to their corresponding NodeInfo.

    Returns:
        Function that parses the value of a node to an Enum if possible.
    """

    @lru_cache
    def get_enum_cached(path: LabOneNodePath) -> NodeEnum | None:
        """Cache based on path."""
        return _get_enum(info=path_to_info[path], path=path)

    def default_enum_parser(annotated_value: AnnotatedValue) -> AnnotatedValue:
        """Default Enum Parser.

        Args:
            annotated_value: Value to be parsed.

        Returns:
            Parsed value.
        """
        try:
            enum = get_enum_cached(annotated_value.path)
        except KeyError:  # pragma: no cover
            # There is no sane scenario where this should happen. But the
            # parser should not raise an exception. Therefore we return the
            # original value.
            logger.warning(  # pragma: no cover
                "Failed to parse the result for %s, its not part of the node tree.",
                annotated_value.path,
            )
            return annotated_value  # pragma: no cover
        if enum is not None and annotated_value.value is not None:
            try:
                annotated_value.value = enum(annotated_value.value)
            except ValueError:  # pragma: no cover
                # The value is not part of the enum. This is a critical error
                # of the server. But the parser should not raise an exception.
                # Therefore we return the original value.
                logger.warning(  # pragma: no cover
                    "Failed to parse the %s for %s, the value is not part of the enum.",
                    annotated_value.value,
                    annotated_value.path,
                )
                return annotated_value  # pragma: no cover
        return annotated_value

    return default_enum_parser
