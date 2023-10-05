| | |
| --- | --- |
| Package | not yet released on pypi.org |
| Meta | [![Hatch project](https://img.shields.io/badge/%F0%9F%A5%9A-Hatch-4051b5.svg)](https://github.com/pypa/hatch) [![linting - Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v0.json)](https://github.com/charliermarsh/ruff) [![code style - Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black) [![types - Mypy](https://img.shields.io/badge/types-Mypy-blue.svg)](https://github.com/python/mypy) [![License - MIT](https://img.shields.io/badge/license-MIT-9400d3.svg)](https://spdx.org/licenses/)|
| CI | ![](https://github.com/zhinst/labone-python/actions/workflows/github-code-scanning/codeql/badge.svg) ![](https://github.com/zhinst/labone-python/actions/workflows/code_quality.yml/badge.svg) ![](https://github.com/zhinst/labone-python/actions/workflows/tests.yml/badge.svg) | 
-----

# LabOne Python API

A python only API for [Zurich Instruments LabOne](https://www.zhinst.com/labone).

Warning:
    This is a work in progress and not yet ready for production use. Feel free
    to try it out and give feedback.

Warning:
    The API is built and tested against the latest internal LabOne development
    version. It will not work with the latest released version of LabOne.

Current development status:

* [x] Basic connection to the data server through the labone.core.session.KernelSession
* [x] Full support of the data server API through the labone.core.session.KernelSession
* [] Async node tree 
* [] Helper functions
* [] Device objects


Warning:
    The name of the project and package are still a work in progress.

## Installation

Warning: 
    Currently, we use the master version of the pycapnp package. Unfortunately,
    pycapnp does not use git for versioning. Therefore, the master version has the
    same then the latest released version. 
    This means it has to be manually installed. To ease the installation, we have
    temporarily created a custom index at https://docs.zhinst.com/pypi/ from which 
    the master version can be installed.

First, install the latest pycapnp master version

```bash
pip install --upgrade --force-reinstall -i https://docs.zhinst.com/pypi/ pycapnp
```

Then install the labone package.

Warning: 
    Since this package has not been released on pypi yet, it has to be installed
    directly from github. This requires the `packaging` package to be installed.
    (`pip install packaging`)

```bash
pip install git+https://github.com/zhinst/labone-python@main
```

# Demo Usage

Compared to the `zhinst` python api, the `labone` python api is a pure python
implementation. It does not depend on the LabOne C API but on the captain proto.
The main differences are:

* Pure Python implementation
* Async
* No single session but individual sessions for each device/kernel

## Data Server Connection

The nodes relevant to the labone software are located under `/zi/*`. They
can be accessed with the `ZIKernelInfo` class.

```python
from labone.core import KernelSession, ZIKernelInfo, ServerInfo, AnnotatedValue

connection = await KernelSession.create(
    kernel_info=ZIKernelInfo(),
    server_info=ServerInfo(host='localhost', port=8004)
)

await connection.list_nodes_info("/zi/*")
```

## Device Connection

To establish a connection to a device, the `DeviceKernelInfo` class is used.
The device id and the interface have to be specified.

```python
from labone.core import KernelSession, DeviceKernelInfo, AnnotatedValue

connection = await KernelSession.create(
    kernel_info=DeviceKernelInfo(device_id="dev1234", interface=DeviceKernelInfo.GbE),
    server_info=ServerInfo(host='localhost', port=8004)
)

await connection.list_nodes_info("*")
```

## Getting And Setting Values

Setting and getting values are all done through the `get` and `set` methods.
Both expect a single node path. If wildcards are used, the `get_with_expression`
and `set_with_expression` methods have to be used.

```python
from labone.core import AnnotatedValue

await connection.get("/zi/debug/level")

await connection.set(AnnotatedValue(path = "/zi/debug/level" value=1))

await connection.get("/zi/debug/level")
```

## Subscription

The most significant change is probably the subscription mechanism. The `subscribe` method
returns a dedicated queue. The server automatically pushes all events for
the subscribed node to the queue. No polling is required.

```python
queue = await connection.subscribe("/zi/debug/level")
await connection.set(AnnotatedValue(path = "/zi/debug/level" value=1))
new_value = await queue.get()
```

Note:
    Every call to subscribe will register a new subscription on the server.
    This means that the server sends the data to all registered queues in 
    separate messages. If you want to have multiple queues for the same node,
    it is recommended to `fork` an existing queue. This will create a new
    **completely independent** queue, but the underlying connection to the
    server is shared.

## Contributing

See [Contributing](CONTRIBUTING.md)
