import datetime as dt

import polars as pl

from novi_tally.connections.file_systems import RemoteFileSystem
from novi_tally.connections.openfigi import OpenFigiApi


class EnfusionLoaderBase:
    def __init__(self, fs: RemoteFileSystem):
        self._fs = fs


class EnfusionPositionLoader(EnfusionLoaderBase):
    def extract(self, date: dt.date, accounts: list[str] | None = None) -> pl.DataFrame:
        path = f"daily_positions/paf_1_dailyposition_{date:%Y%m%d}.csv"

        data = self._fs.read_bytes(path)
        raw = (
            pl.read_csv(
                data,
                schema_overrides={
                    "Deal Id": pl.String,
                    "Notional Quantity": pl.Float32,
                },
            )
            .filter(
                pl.col("Position Scenario Date").is_not_null(),
                pl.col("Active"),
            )
            .with_columns(
                pl.when(pl.col("Account").str.starts_with("IBLLC"))
                # extract IB account number like "IBLLC U8674826"
                .then(pl.col("Account").str.split(" ").list.get(-1))
                .otherwise(
                    # extract RJO account number like
                    # RJO' Brien Bank A/c: 791 30014
                    # RJO' Brien Bank A/c: 791 30013 - F1
                    # RJO' Brien Bank A/c: 791-30012 - F1 (are you kidding me..?)
                    pl.when(pl.col("Account").str.starts_with("RJO"))
                    .then(
                        pl.col("Account")
                        .str.split(" - ")
                        .list.get(0)
                        .str.split(" ")
                        .list.get(-1)
                        .str.split("-")
                        .list.get(-1)
                    )
                    # otherwise keep the original
                    .otherwise(pl.col("Account"))
                )
                .alias("account_id"),
            )
        )
        if accounts is not None:
            raw = raw.filter(pl.col("account_id").is_in(accounts))

        return raw

    def transform(self, raw: pl.DataFrame) -> pl.DataFrame:
        return (
            raw.filter(pl.col("BB Yellow Key").is_not_null())
            .group_by("account_id", "BB Yellow Key")
            .agg(
                pl.col("Notional Quantity").sum().cast(pl.Int64).alias("quantity"),
                pl.col("Market Price").first().alias("price"),
                pl.col("Native Currency").first().alias("local_ccy"),
                pl.col("Description").first().alias("description"),
            )
            .select(
                pl.col("BB Yellow Key").str.to_uppercase().alias("bbg_yellow"),
                pl.col("account_id"),
                pl.col("description"),
                pl.col("quantity"),
                pl.col("price"),
                pl.col("local_ccy"),
            )
        )
