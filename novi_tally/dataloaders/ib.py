import datetime as dt

import polars as pl
import pandas as pd
from typing import Dict, List
from io import StringIO

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
                pl.col("CostPrice").first().alias("cost_price_lc"),
                pl.col("Currency").first().alias("local_ccy"),
                pl.col("BBGlobalID").first(),
                pl.col("AssetType").first().alias("asset_type"),
                pl.col("Multiplier").first().alias("multiplier"),
            )
            .select(
                pl.col("AccountID").alias("account_id"),
                pl.col("SecurityDescription").alias("description"),
                pl.col("BBGlobalID"),
                pl.col("quantity"),
                pl.col("price"),
                pl.col("local_ccy"),
                pl.col("asset_type"),
                pl.col("cost_price_lc"),
                pl.col("multiplier"),
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


class IbMarginLoader(IbLoaderBase):
    def extract(self, date: dt.date, accounts: list[str] | None = None) -> pl.DataFrame:
        path_margin = f"IB/F5678557_Margin_{date:%Y%m%d}.csv"

        data = self._fs.read_bytes(path_margin)

        account_tables = self._get_account_tables(data.decode())

        margin_summary_table_list = []
        for account, tables in account_tables.items():
            margin_summary_df = tables["MarginSummary"]
            int_cols = margin_summary_df.select_dtypes(include="int").columns
            margin_summary_df[int_cols] = margin_summary_df[int_cols].astype("float")
            pl_df = pl.from_pandas(margin_summary_df)
            pl_df = pl_df.with_columns(pl.lit(account).alias("AccountID"))
            margin_summary_table_list.append(pl_df)

        raw = pl.concat(margin_summary_table_list, how="vertical")
        if accounts is not None:
            raw = raw.filter(pl.col("AccountID").is_in(accounts))

        raw = raw.pivot(
            values="Total",
            index="AccountID",
            on="Parameter",
        )
        return raw

    def transform(self, raw: pl.DataFrame) -> pl.DataFrame:
        return raw.select(
            pl.col("AccountID").alias("account_id"),
            pl.col("InitialMarginRequirement").alias("initial_margin"),
            pl.col("NetLiquidationValue").alias("liquidating_value"),
        )

    def _get_account_tables(self, str_data: str) -> Dict[str, Dict[str, pd.DataFrame]]:
        lines = str_data.split("\n")

        account_tables = dict()

        account = None
        section_lines = []

        for line in lines:
            line = line.strip()
            if line.startswith("BOF"):
                account = line.split(",")[1].strip()
                section_lines = []
                continue
            elif line.startswith("EOF"):
                if section_lines:
                    account_tables[account] = self._get_tables_from_section(
                        section_lines
                    )
                continue
            else:
                section_lines.append(line)

        return account_tables

    def _is_header_line(self, line: str) -> bool:
        return "Header" in line.split(",")

    def _read_df_from_table_lines(
        self, names: List[str], table_lines: List[str]
    ) -> pd.DataFrame:
        table_str = "\n".join(table_lines)
        df = pd.read_csv(StringIO(table_str), names=names)
        return df

    def _get_tables_from_section(
        self, section_lines: List[str]
    ) -> Dict[str, pd.DataFrame]:
        last_header = None
        tables = {}

        table_lines = []
        for line in section_lines:
            if self._is_header_line(line):
                if last_header is not None and table_lines:
                    names = last_header.split(",")
                    df = self._read_df_from_table_lines(names, table_lines)
                    tables[names[0]] = df
                    table_lines = []

                last_header = line
            else:
                table_lines.append(line)

        if last_header is not None and table_lines:
            names = last_header.split(",")
            df = self._read_df_from_table_lines(names, table_lines)
            tables[names[0]] = df

        return tables
