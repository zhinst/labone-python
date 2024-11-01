| | |
| --- | --- |
| Package | [![PyPI version](https://badge.fury.io/py/labone.svg)](https://badge.fury.io/py/labone) |
| Meta | [![Hatch project](https://img.shields.io/badge/%F0%9F%A5%9A-Hatch-4051b5.svg)](https://github.com/pypa/hatch) [![linting - Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v0.json)](https://github.com/charliermarsh/ruff) [![code style - Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black) [![types - Mypy](https://img.shields.io/badge/types-Mypy-blue.svg)](https://github.com/python/mypy) [![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)|
| CI | ![](https://github.com/zhinst/labone-python/actions/workflows/github-code-scanning/codeql/badge.svg) ![](https://codecov.io/gh/zhinst/labone-python/branch/main/graph/badge.svg?token=VUDDFQE20M) ![](https://github.com/zhinst/labone-python/actions/workflows/code_quality.yml/badge.svg) ![](https://github.com/zhinst/labone-python/actions/workflows/tests.yml/badge.svg) |
-----

# LabOne Python API

The `labone` package provides a plain asynchronous Python API for [LabOne](https://www.zhinst.com/labone), the control software of Zurich Instruments.

> [!CAUTION]
> This API package is solely being developed to support [LabOne Q](https://www.zhinst.com/quantum-computing-systems/labone-q), the software framework for quantum computing.
>
> For direct access to the instruments without LabOne Q, the standard Python API for all Zurich Instruments' devices is provided through the `zhinst` package and can be obtained from [PyPI](https://pypi.org/project/zhinst/) by `pip install zhinst`.
>

> [!NOTE]
> Since `labone` is not intended for direct usage, we do not offer any support
> or external documentation. Please contact [Zurich Instruments](mailto:info@zhinst.com) if you have any questions.

## Internal Documentation

The internal documentation can be found [here](http://docs.pages.zhinst.com/internal-documentation-hub/async_labone/index.html).
Due to the early stage there is not public documentation.

## Contributing

See [Contributing](CONTRIBUTING.md)
