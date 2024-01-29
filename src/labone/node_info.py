"""Brings node info into a more usable format."""

from __future__ import annotations

import re
import typing as t
from functools import cached_property

if t.TYPE_CHECKING:
    from labone.core.helper import LabOneNodePath
    from labone.core.session import NodeInfo as NodeInfoType
    from labone.core.session import NodeType


def _parse_option_keywords_description(option_string: str) -> tuple[list[str], str]:
    r"""Parse the option string into keywords and description.

    Infos for enumerated nodes come with a string for each option.
    This function parses this string into relevant information.

    There are two valid formats for the option string:
    1. Having a single keyword:
        e.g. "Alive"
        -> keyword: ["Alive"], description: ""

    2. Having one or multiple keywords in parenthesis,
        optionally followed by a colon and a description:
        e.g. "\"sigin0\", \"signal_input0\": Sig In 1"
        -> keyword: ["sigin0", "signal_input0"], description: "Sig In 1"

    Args:
        option_string: String, which should be parsed.

    Returns:
        List of keywords and the description.
    """
    # find all keywords in parenthesis
    matches = list(re.finditer(r'"(?P<keyword>[a-zA-Z0-9-_"]+)"', option_string))
    options = [option_string] if not matches else [m.group("keyword") for m in matches]

    # take everythin after ": " as the description if present
    description_match = re.search(r": (.*)", option_string)
    description = description_match.group(1) if description_match else ""

    return options, description


class OptionInfo(t.NamedTuple):
    """Representing structure of options in NodeInfo."""

    enum: str
    description: t.Any


class NodeInfo:
    """Encapsulating information about a leaf node.

    This class contains all information about a node provided by the server.
    This includes:

    * LabOne node path
    * Description of the node
    * Properties of the node (e.g. Read, Write, Setting)
    * Type of the node (e.g. Double, Integer, String)
    * Unit of the node, if applicable (e.g. V, Hz, dB)
    * Options of the node, if applicable (e.g. 0: "Off", 1: "On")

    Args:
        info: Raw information about the node (JSON formatted), as provided by
            the server.
    """

    def __init__(self, info: NodeInfoType):
        self._info: NodeInfoType = info

    @classmethod
    def plain_default_info(cls, *, path: str) -> NodeInfoType:
        """Get default info for a node.

        Returns:
            Default info for a node.
        """
        return {
            "Description": "",
            "Properties": "Read, Write",
            "Unit": "None",
            "Node": path,
        }  # type: ignore[typeddict-item]

    def _checked_dict_access(
        self,
        item: str,
    ) -> LabOneNodePath | str | NodeType | dict[str, str]:
        """Indirect access to underlying dictionary.

        The underlying dictionary may not be complete in some cases.
        This function raises an appropriate error if an required key is missing.

        Args:
            item: Key to access.

        Returns:
            Value of the key.

        Raises:
            KeyError: If the key is valid but not present in the dictionary.

        """
        if item not in self._info and item in [
            "Description",
            "Node",
            "Properties",
            "Type",
            "Unit",
            "Options",
        ]:
            msg = f"NodeInfo is incomplete. As '{item}'\
              is missing, not all behavior is available."
            raise KeyError(msg)
        return self._info[item]  # type: ignore[literal-required]

    def __getattr__(
        self,
        item: str,
    ) -> LabOneNodePath | str | NodeType | dict[str, str] | None:
        return self._checked_dict_access(item.capitalize())

    def __dir__(self) -> list[str]:
        return [k.lower() for k in self._info] + [
            var
            for var, value in vars(self.__class__).items()
            if isinstance(value, property) and not var.startswith("_")
        ]

    def __repr__(self) -> str:
        return f'NodeInfo({self._info.get("Node", "unknown path")})'

    def __str__(self) -> str:
        string = self._info.get("Node", "unknown path")
        string += "\n" + self._info.get("Description", "(no description available)")
        for key, value in self._info.items():
            if key == "Options":
                string += f"\n{key}:"
                for option, description in value.items():  # type: ignore[attr-defined]
                    string += f"\n    {option}: {description}"
            elif key not in ["Description", "Node", "SetParser", "GetParser"]:
                string += f"\n{key}: {value}"
        return string

    @property
    def readable(self) -> bool:
        """Flag if the node is readable."""
        return "Read" in self._checked_dict_access("Properties")  # type: ignore[return-value]

    @property
    def writable(self) -> bool:
        """Flag if the node is writable."""
        return "Write" in self._checked_dict_access("Properties")  # type: ignore[return-value]

    @property
    def is_setting(self) -> bool:
        """Flag if the node is a setting."""
        return "Setting" in self._checked_dict_access("Properties")  # type: ignore[return-value]

    @property
    def is_vector(self) -> bool:
        """Flag if the value of the node a vector."""
        return "Vector" in self._checked_dict_access("Type")  # type: ignore[return-value]

    @property
    def path(self) -> str:
        """LabOne path of the node."""
        return self._checked_dict_access("Node").lower()  # type: ignore[return-value, union-attr]

    @property
    def description(self) -> str:
        """Description of the node."""
        return self._checked_dict_access("Description")  # type: ignore[return-value]

    @property
    def type(self) -> str:
        """Type of the node."""
        return self._checked_dict_access("Type")  # type: ignore[return-value]

    @property
    def unit(self) -> str:
        """Unit of the node."""
        return self._checked_dict_access("Unit")  # type: ignore[return-value]

    @cached_property
    def options(self) -> dict[int, OptionInfo]:
        """Option mapping of the node."""
        options_mapping = {}
        for key, option_string in self._info.get("Options", {}).items():
            options, description = _parse_option_keywords_description(option_string)

            # Only use the first keyword as the enum value
            options_mapping[int(key)] = OptionInfo(
                enum=options[0],
                description=description,
            )
        return options_mapping

    @property
    def as_dict(self) -> NodeInfoType:
        """Underlying dictionary."""
        return self._info
