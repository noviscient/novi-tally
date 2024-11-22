# NOVI TALLY

A module to reconcile positions and trades at Noviscient.

## Quick Start

### Position

`Position` is a class you will use directly to reconcile positions.

It can be initialized either with:

- a builtin or custom [dataloader](#data-loaders)
- a builtin provider with a [config file](#configuration)

```python
from novi_tally import Position


date = datetime.date(2024, 1, 1)

# from a config file
ib_position = Position.from_config_file(
    provider="ib",
    date=date,
    config_filepath='path_to_config_file',
)

# with a dataloader
my_dataloader = MyDataLoader(...)
local_position = Position(
    dataloader=my_dataloader,
    date=date,
    provider_name="local",
)

# reconcile: check the docstring for `reconcile_with` method
ib_position.reconcile_with(local_position, instrument_identifier="description")
```

### Trade

TODO

## Underlying Concepts

### Data Loaders

Each data loader is a class compatible to a protocol, which defines the
following methods:

- `extract`: Download the raw data.
- `transform`: transform the raw data according to a [schema](#schemas).

Refer to their [definitions](./novi_tally/protocols.py) for details.

### Schemas

A schema defines the columns and their types / constraints in a standardized
dataframe ready for reconciliation.

Refer to their [definitions](./novi_tally/schemas.py) for details.

### Connections

A connection class wraps the basic function to retrieve data from a certain source.

The library comes with the following connections:

- AWS S3
- SFTP
- OPENFIGI API

The builtin [dataloaders](#data-loaders) use them to retrieve necessary data.
    Use them for your custom dataloaders when appropriate.

## Configuration

See the provided [example config file](./config-example.toml).

## Contribute

### TODO

Unfinished items with high priorities:

- [ ] Incorporate Formidium API for Formidium dataloader
    (currently read from a local file, see [here](./novi_tally/dataloaders/formidium.py))
- [ ] Follow the pattern for position reconciliation, implement trade reconciliation.

### Guide

#### Adding a new dataloader

The only two requirements for a new dataloader class are:

- it should conform to the corresponding [protocol](./novi_tally/protocols.py).
- the dataframe returned by its `transform` method should conform to the
    corresponding [schema](./novi_tally/schemas.py)

When the data can't be retrieved from existing connections, you may also want to consider
contributing a new connection if you deem it could be used by others in the future.

For example:

- We want to add position dataloaders for two new brokers: Foo and Bar;
- Data for broker Foo also sits on AWS S3, but data for broker Bar needs to be retrieved
    by a new API

For broker Foo:

```python
# ./novi_tally/dataloaders/foo.py

class FooPositionLoader:
    # utilize the provided S3 connection wrapper
    def __init__(self, fs: S3FileSystem):
        self._fs = fs
        ...

    def extract(self, date, accounts):
        # use `self._fs` here
        ...

    def transform(self, raw):
        # make sure the returned dataframe conforms to the position schema
        ...
```

For broker Bar:

```python
# ./novi_tally/connections/new_api.py

class NewApi:
    def __init__(self, some_secret):
        ...
```

```python
# ./novi_tally/dataloaders/boo.py

class BarPositionLoader:
    # utilize the provided S3 connection wrapper
    def __init__(self, api: NewApi):
        self._api = api
        ...

    def extract(self, date, accounts):
        # use self._api here
        ...

    def transform(self, raw):
        # make sure the returned dataframe conforms to the position schema
        ...
```
