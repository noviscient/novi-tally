# NOVI TALLY

A module to reconcile positions and trades at Noviscient.

## Quick Start

### Position

`Position` is a class you will use directly to reconcile positions.

It can be initialized either with:

- a builtin provider(which requires a proper [config file](#configuration) is loaded)
- a custom [dataloader](#data-loaders)

```python
from novi_tally import Position


date = datetime.date(2024, 1, 1)

# with a builtin provider
ib_position = Position(date=date, provider="IB")

# with a custom dataloader
my_dataloader = MyDataLoader(...)
formidium_position = Position(date=date, provider="Custom", dataloader=my_dataloader)

# reconcile
ib_position.reconcile_with(formidium_position, instrument_identifier="description")
```

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

### Sources

The library comes with the following sources:

## Configuration
