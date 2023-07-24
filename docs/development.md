# Development

This project uses [Hatch](https://hatch.pypa.io/latest/) for development management.

## Installing requirements

```bash
pip install -r requirements.txt
```

## Running tests

### Package tests

```bash
hatch run test:pytest
```

### Code style tests

```bash
hatch run style:check
```

### Building the package

```bash
hatch build
```
