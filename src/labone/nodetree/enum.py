"""Defining Enum-Handling behavior."""

from __future__ import annotations

import re
import typing as t
from enum import Enum, IntEnum
from functools import lru_cache

import numpy as np

if t.TYPE_CHECKING:
    from labone.core import AnnotatedValue  # pragma: no cover
    from labone.core.helper import LabOneNodePath  # pragma: no cover
    from labone.core.session import NodeInfo as NodeInfoType  # pragma: no cover


T = t.TypeVar("T")


class NodeEnumMeta:
    """Custom Metaclass for NodeEnum.

    Note: Required to enable pickling of a NodeEnum value.

    It simply servers the purpose to recreate a NodeEnum for a given enum
    value. Since the NodeEnums are created dynamically there is no way recreate
    a NodeEnum value since python can not find the definition. This class
    bypasses this problem by providing the functionality to recreate the
    Enum on the fly.

    Warning: Although the class of the resulting enum object looks and feels
    the same as the original one it is not. Therefore comparing the `type` will
    fail. This is however the only limitation.
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
    the node informations. Since they are not predefined but rather created
    dynamically, the are not picklable. This custom child class of IntEnum
    overwrites the reduce function that returns all information required to
    recreate the Enum class in `NodeEnumMeta`.

    For more information on the reduce functionality and how it is used within
    the pickle package see
    [pep307](https://peps.python.org/pep-0307/#extended-reduce-api).
    """

    # Required for typing
    def __init__(self, *args, **kwargs) -> None:
        ...  # pragma: no cover

    # Required for typing
    def __call__(self, *args, **kwargs) -> None:  # noqa: D102 # pragma: no cover
        ...  # pragma: no cover

    def __reduce_ex__(
        self,
        _: object,
    ) -> tuple[type[NodeEnumMeta], tuple[int, str, dict, str]]:
        return NodeEnumMeta, (
            self._value_,
            self.__class__.__name__,
            {key: int(value) for key, value in self.__class__._member_map_.items()},  # type: ignore[call-overload] # noqa: SLF001
            self.__class__.__module__,
        )


def _get_enum(*, info: NodeInfoType, path: LabOneNodePath) -> NodeEnum | None:
    """Enum of the node options."""
    if "Options" not in info:
        return None

    options_reversed = {}
    for int_key, value in info.get("Options", {}).items():
        # Find all the keywords associated to a integer key
        # account for formats like: "\"sigin0\", \"signal_input0\": Sig In 1"
        matches = list(re.finditer(r'"(?P<keyword>[a-zA-Z0-9-_"]+)"', value))

        if not matches:
            # account for plain formats like: "Alive"
            options_reversed[value] = int_key
        for m in matches:
            keyword = m.group("keyword")
            options_reversed[keyword] = int_key

    return NodeEnum(path, options_reversed, module=__name__)


def get_default_enum_parser(
    path_to_info: dict[LabOneNodePath, NodeInfoType],
) -> t.Callable[[AnnotatedValue], AnnotatedValue]:
    """Default Enum Parser Closure.

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
        is_integer_value = isinstance(annotated_value.value, (int, np.integer))

        if is_integer_value and annotated_value.path in path_to_info:
            enum = get_enum_cached(annotated_value.path)
            if enum is not None:
                annotated_value.value = enum(annotated_value.value)
        return annotated_value

    return default_enum_parser
