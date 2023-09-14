# LabOne Python API

A python only API for [Zurich Instruments LabOne](https://www.zhinst.com/labone).

Warning:
    This is a work in progress and not yet ready for production use. Feel free
    to try it out and give feedback.

Current development status:

* [x] Basic connection to the data server through the labone.core.session.KernelSession
* [x] Full support of the data server API through the labone.core.session.KernelSession
* [] Async Nodetree 
* [] Helper functions
* [] Device objects


Warning:
    The name of the project and package are still work in progress.

## Installation

Warning: 
    Currently, we use the master version of the pycapnp package. Unfortunately
    pycapnp does not use git for versioning. Therefore, the master version has the
    same then the latest released on. 
    This means it has to be manually installed. To ease the installation, we have
    temporarily created a custom index at https://docs.zhinst.com/pypi/ from which 
    the master version can be installed.

First, install the latest pycapnp master version
(if you have already installed pycapnp, please uninstall it first)

```bash
pip install --upgrade --force-reinstall -i https://docs.zhinst.com/pypi/ pycapnp
```

Then install the labone package

```bash
pip install git+https://github.com/zhinst/labone-python@main
```

# Demo Usage

## Data Server connection

```python
from labone.core import KernelSession, ZIKernelInfo, ServerInfo, AnnotatedValue

connection = await KernelSession.create(
    kernel_info=ZIKernelInfo(),
    server_info=ServerInfo(host='localhost', port=8004)
)

await connection.list_nodes_info("/zi/*")
```

## Device connection

```python
from labone.core import KernelSession, DeviceKernelInfo, AnnotatedValue

connection = await KernelSession.create(
    kernel_info=DeviceKernelInfo(device_id="dev1234", interface=DeviceKernelInfo.GbE),
    server_info=ServerInfo(host='localhost', port=8004)
)

await connection.list_nodes_info("*")
```

## Contributing

See [Contributing](CONTRIBUTING.md)
