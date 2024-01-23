# Contributing

We welcome any contribution. Since the project is currently not intended for direct
use we do not offer support or external documentation.

## Development

This project uses [Hatch](https://hatch.pypa.io/latest/) for development management.

### Install initial requirements

```bash
pip install -r requirements.txt
```

### Running tests

### Package tests

```bash
hatch run test:test
```

### Code style tests

```bash
hatch run lint:fmt
hatch run lint:typing
```

### Building the package

```bash
hatch build
```
