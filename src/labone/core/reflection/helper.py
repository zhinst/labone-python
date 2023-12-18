"""Helper module for the reflection module."""
import os
import sys
import typing as t
from contextlib import contextmanager


@contextmanager
def suppress_std_err() -> t.Generator[None, None, None]:
    """Suppress stderr for the duration of the context.

    In contrast to the `contextlib.redirect_stderr()` this also works for
    libraries that use the C-API for printing to stderr. This makes it
    useful for suppressing the err output of the capnp library.

    Warning: Everything that is printed to stderr during the context will be
        lost. This can be problematic if the suppressed output contains
        important information.
    """
    fd_stderr = sys.stderr.fileno()

    with os.fdopen(os.dup(fd_stderr), "w") as old_stderr:
        # redirect stderr to devnull
        with open(os.devnull, "w") as file:  # noqa: PTH123
            sys.stderr.close()
            os.dup2(file.fileno(), fd_stderr)
            sys.stderr = os.fdopen(fd_stderr, "w")
        try:
            yield
        finally:
            # restore stderr to its previous value
            sys.stderr.close()
            os.dup2(old_stderr.fileno(), fd_stderr)
            sys.stderr = os.fdopen(fd_stderr, "w")
