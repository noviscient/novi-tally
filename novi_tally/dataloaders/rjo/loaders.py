import datetime as dt
from typing import Any

import polars as pl

from novi_tally.connections.file_systems import RemoteFileSystem

from . import headers


def make_bloomberg_yellow_code(row: dict[str, Any]) -> str:
    description = row["Security_desc_line_1"]
    bloomberg_root = row["bloomberg_root"]
    bloomberg_market_sector = row["bloomberg_market_sector"]
    contract_month = row["Contract_month"]

    month_to_code = {
        "01": "F",
        "02": "G",
        "03": "H",
        "04": "J",
        "05": "K",
        "06": "M",
        "07": "N",
        "08": "Q",
        "09": "U",
        "10": "V",
        "11": "X",
        "12": "Z",
    }

    if description.endswith("SCM TSR20RUBBR"):
        bloomberg_root = "OR"
        bloomberg_market_sector = "Comdty"
    elif description.endswith("LME NICKEL US"):
        bloomberg_root = "LN"
        bloomberg_market_sector = "Comdty"
    elif description.endswith("CME LUMBER FUT"):
        bloomberg_root = "LBO"
        bloomberg_market_sector = "Comdty"
    # HACK: Temporary fix for RJO wrong symbol
    elif bloomberg_root == "NZ" and bloomberg_market_sector == "Index":
        bloomberg_root = "JGS"
    elif bloomberg_root == "AL" and bloomberg_market_sector == "Comdty":
        bloomberg_root = "ALE"
    elif description.endswith("ICE UKA FUT"):
        bloomberg_root = "UKE"
        bloomberg_market_sector = "Comdty"
    elif description.endswith("EUX FDXS FUT"):
        bloomberg_root = "MZS"
        bloomberg_market_sector = "Index"
    elif description.endswith("OSE GOLD"):
        bloomberg_root = "JG"
        bloomberg_market_sector = "Comdty"
    elif description.endswith("CMX MHG COPPER"):
        bloomberg_root = "MHC"
        bloomberg_market_sector = "Comdty"
    elif description.endswith("NYM MICR CRUDE"):
        bloomberg_root = "WMI"
        bloomberg_market_sector = "Comdty"
    elif description.endswith("IFLL 3MESRT F"):
        bloomberg_root = "TKY"
        bloomberg_market_sector = "Comdty"
    elif description.endswith("ICE FTSE250 2"):
        bloomberg_root = "YBY"
        bloomberg_market_sector = "Index"

    bloomberg_root = bloomberg_root
    sector = bloomberg_market_sector
    if len(bloomberg_root) == 1:
        bloomberg_root += " "

    contract_month = contract_month
    year, month = contract_month[:4], contract_month[4:]
    month_code = month_to_code[month]
    if bloomberg_root in (
        "NG",
        "LA",
        "MO",
    ):
        year_code = year[-2:]
    else:
        year_code = year[-1]

    bbg_code = bloomberg_root + month_code + year_code + " " + sector

    return bbg_code


class RjoLoaderBase:
    def __init__(self, fs: RemoteFileSystem):
        self._fs = fs


class RjoPositionLoader(RjoLoaderBase):
    def extract(self, date: dt.date, accounts: list[str] | None = None) -> pl.DataFrame:
        path = f"NOVISCIENT_SFTP_csvnpos_npos_{date:%Y%m%d}.csv"
        data = self._fs.read_bytes(path)

        filters = [
            pl.col("Record_code") == "P",
        ]
        if accounts:
            filters.append(pl.col("Account_number").is_in(accounts))

        raw = pl.read_csv(
            data,
            has_header=False,
            new_columns=headers.POSITION_HEADER,
            schema_overrides={"Contract_month": pl.String, "Account_number": pl.String},
        ).filter(filters)

        return raw

    def transform(self, raw: pl.DataFrame) -> pl.DataFrame:
        return (
            raw.filter(
                pl.col("Security_desc_line_1").is_not_null(),
                # filter out options with non-empty Security_subtype_code
                pl.col("Security_subtype_code").is_null(),
            )
            .group_by("Account_number", "Security_desc_line_1")
            .agg(
                (pl.col("Quantity") * pl.col("Buy_sell_code").replace({2: -1}))
                .sum()
                .cast(pl.Int64)
                .alias("quantity"),
                pl.col("Close_price").first().alias("price"),
                pl.col("Account_type_currency_symbol").first().alias("local_ccy"),
                pl.col("Contract_month").first(),
                pl.col("bloomberg_root").first(),
                pl.col("bloomberg_market_sector").first(),
            )
            .select(
                pl.struct(pl.all())
                .map_elements(make_bloomberg_yellow_code, return_dtype=pl.String)
                .str.to_uppercase()
                .alias("bbg_yellow"),
                pl.col("Account_number").alias("account_id"),
                pl.col("Security_desc_line_1").alias("description"),
                pl.col("quantity"),
                pl.col("price"),
                pl.col("local_ccy"),
            )
        )
