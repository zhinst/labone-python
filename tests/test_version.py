"""Test module for the package version."""

import labone


def test_pkg_version():
    assert labone.__version__ == labone._version.__version__
