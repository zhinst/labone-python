| | |
| --- | --- |
| Package | not yet released on pypi.org |
| Meta | [![Hatch project](https://img.shields.io/badge/%F0%9F%A5%9A-Hatch-4051b5.svg)](https://github.com/pypa/hatch) [![linting - Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v0.json)](https://github.com/charliermarsh/ruff) [![code style - Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black) [![types - Mypy](https://img.shields.io/badge/types-Mypy-blue.svg)](https://github.com/python/mypy) [![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)|
| CI | ![](https://github.com/zhinst/labone-python/actions/workflows/github-code-scanning/codeql/badge.svg) ![](https://codecov.io/gh/zhinst/labone-python/branch/main/graph/badge.svg?token=VUDDFQE20M) ![](https://github.com/zhinst/labone-python/actions/workflows/code_quality.yml/badge.svg) ![](https://github.com/zhinst/labone-python/actions/workflows/tests.yml/badge.svg) | 
-----

# LabOne Python API

A python-only API for [Zurich Instruments LabOne](https://www.zhinst.com/labone).

Warning:

    This is a work in progress and may never be release. 

Warning:
    The API is built and tested against the latest internal LabOne development
    version. It will most likely not work with the latest released version of LabOne.

Current development status:

* [x] Basic connection to the data server through the labone.core.session.KernelSession
* [x] Full support of the data server API through the labone.core.session.KernelSession
* [x] Async lazy node tree 
* [x] Device objects
* [ ] feature parity with zhinst-toolkit

## Installation

Warning: 
    Since this package has not been released on pypi yet, it has to be installed
    directly from github. This requires the `packaging` package to be installed.
    (`pip install packaging`)

```bash
pip install git+https://github.com/zhinst/labone-python@main
```

# Demo Usage

Take a look at the [getting started](getting_started.md) page for a short introduction

## Contributing

See [Contributing](CONTRIBUTING.md)
