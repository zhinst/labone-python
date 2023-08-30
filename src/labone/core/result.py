"""T O D O error handling."""
import capnp

from labone.core import errors
from labone.core.resources import (  # type: ignore[attr-defined]
    result_capnp,
)


def unwrap(
    result: result_capnp.Result,
) -> capnp.lib.capnp._DynamicStructReader:  # noqa: SLF001
    """Unwrap a result."""
    try:
        return result.ok
    except capnp.KjException:
        pass
    try:
        raise errors.LabOneCoreError(result.err.msg)
    except capnp.KjException as e:
        msg = "Whoops"
        raise RuntimeError(msg) from e
