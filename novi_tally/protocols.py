import datetime as dt
from typing import Protocol

import polars as pl


class PositionLoader(Protocol):
    def extract(
        self, date: dt.date, accounts: list[str] | None = None
    ) -> pl.DataFrame: ...

    def transform(self, raw: pl.DataFrame) -> pl.DataFrame: ...


class TradeLoader(Protocol):
    def extract(
        self, start: dt.date, end: dt.date, accounts: list[str] | None = None
    ) -> pl.DataFrame: ...

    def transform(self, raw: pl.DataFrame) -> pl.DataFrame: ...
