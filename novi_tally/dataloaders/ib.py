import datetime as dt

import polars as pl

from novi_tally.connections.file_systems import RemoteFileSystem
from novi_tally.connections.openfigi import OpenFigiApi


class IbLoaderBase:
    def __init__(self, fs: RemoteFileSystem, openfigi_api: OpenFigiApi):
        self._fs = fs
        self._openfigi_api = openfigi_api


class IbPositionLoader(IbLoaderBase):
    def extract(self, date: dt.date, accounts: list[str] | None = None) -> pl.DataFrame:
        path = f"IB/F5678557_Position_{date:%Y%m%d}.csv"
        data = self._fs.read_bytes(path)

        filters = [
            pl.col("Type") == "D",
            pl.col("AssetType").is_not_null(),
            # refer to IB's doc for all asset types:
            # https://www.ibkrguides.com/reportingintegration/topics/asset_types.htm
            ~pl.col("AssetType").is_in(["CASH", "DIVACC", "INTACC"]),
        ]

        if accounts is not None:
            filters.append(pl.col("AccountID").is_in(accounts))

        raw = pl.read_csv(data, skip_rows=1, ignore_errors=True).filter(filters)

        return raw

    def transform(self, raw: pl.DataFrame) -> pl.DataFrame:
        transformed = (
            raw.filter(
                pl.col("SecurityDescription").is_not_null(),
            )
            .group_by("AccountID", "SecurityDescription")
            .agg(
                pl.col("Quantity").sum().cast(pl.Int64).alias("quantity"),
                pl.col("MarketPrice").first().alias("price"),
                pl.col("Currency").first().alias("local_ccy"),
                pl.col("BBGlobalID").first(),
            )
            .select(
                pl.col("AccountID").alias("account_id"),
                pl.col("SecurityDescription").alias("description"),
                pl.col("BBGlobalID"),
                pl.col("quantity"),
                pl.col("price"),
                pl.col("local_ccy"),
            )
        )

        mapping_table = self._openfigi_api.get_bbg_mapping_table(
            transformed["BBGlobalID"]
        )

        transformed = transformed.with_columns(
            pl.col("BBGlobalID")
            .replace(mapping_table)
            .str.to_uppercase()
            .alias("bbg_yellow")
        ).drop("BBGlobalID")

        return transformed
