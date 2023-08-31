# Python API for Zurich Instruments LabOne software

Python API for LabOne.

> **Warning**
The name of the project and package are still work in progress.

## Contributing

See [Contributing](CONTRIBUTING.md)

# Demo Usage

## Data Server connection

```python
from labone.core import Session, ZIKernelInfo, ServerInfo, AnnotatedValue

connection = await Session.create(kernel_info=ZIKernelInfo(), server_info=ServerInfo(host='localhost', port=8004))

await connection.list_nodes_info("/zi/*")
await connection.set_value(AnnotatedValue(path="/node/path", value=123))
```

## Device connection

```python
from labone.core import Session, DeviceKernelInfo, AnnotatedValue

connection = await Session.create(
    kernel_info=DeviceKernelInfo(device_id="dev1234", interface=DeviceKernelInfo.GbE),
    server_info=ServerInfo(host='localhost', port=8004)
)

await connection.list_nodes_info("*")
await connection.set_value(AnnotatedValue(path="/node/path", value=123))
```
