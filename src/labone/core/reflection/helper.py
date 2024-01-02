"""Helper module for the reflection module."""
import os
import typing as t
from contextlib import contextmanager


@contextmanager
def enforce_pwd() -> t.Generator[None, None, None]:
    """Enforces the PWD environment variable to be equal to the pythons cwd.

    This context manager temporarily sets the PWD environment variable to the
    current working directory, defined in the python os library. This is useful
    when c++ code is called from python, which relies on the PWD environment
    variable to be set correctly.
    """
    old_pwd = os.environ.get("PWD")
    try:
        os.environ["PWD"] = os.getcwd()  # noqa: PTH109
        yield
    finally:
        if old_pwd is not None:
            os.environ["PWD"] = old_pwd
        else:
            del os.environ["PWD"]  # pragma: no cover
