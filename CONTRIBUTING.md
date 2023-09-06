# Contributing

We welcome any contribution. If you plan to add a bigger change, please get in
touch with the maintainer first.

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
hatch run lint:style
hatch run lint:typing
```

### Building the package

```bash
hatch build
```
