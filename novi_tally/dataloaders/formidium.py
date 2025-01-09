import datetime as dt
import polars as pl

from novi_tally.connections.formidium import FormidiumApi


class Broker(pl.Enum):
    RJO = "RJO"
    IB = "IB"


class FormidiumLoaderBase:
    def __init__(self, formidium_api: FormidiumApi):
        self._formidium_api = formidium_api


class FormidiumAPIPositionLoader(FormidiumLoaderBase):
    FUNDNAMES = {
        "Noviscient Pure Alpha - Noviscient Solutions VCC": [
            "U19923882",
            "U8674826",
            "30012",
            "30014",
            "30015",
            "30016",
        ],
        "Anar capital Fund - Noviscient Solution VCC": [
            "U11022080",
            "U11027852",
            "U19728903",
        ],
        "Alba Capital Volatility Strategies Fund - Noviscient Solutions VCC": [
            "U15108315",
            "U15188068",
            "U15793786",
        ],
    }

    def _get_fund_name(self, accounts: list[str]) -> str:
        for account in accounts:
            for fund_name, accounts in self.FUNDNAMES.items():
                if any(account == item for item in accounts):
                    return fund_name
        return ""

    def extract(self, date: dt.date, accounts: list[str] | None = None) -> pl.DataFrame:
        if accounts is None:
            return pl.DataFrame()

        broker_names = {
            "RJO' Brien Bank A/c": Broker.RJO,
            "Interactive Brokers": Broker.IB,
        }

        fund_name = self._get_fund_name(accounts)
        data = self._formidium_api.read_positions(
            fund_name=fund_name,
            date=date,
        )
        raw = (
            pl.DataFrame(data["resultList"])
            .filter(pl.col("Symbol").str.len_chars() > 0)
            .with_columns(
                pl.col("Account")
                .str.split(" - ")
                .list.get(0)
                .replace(broker_names)
                .alias("broker"),
            )
            .with_columns(
                pl.when(pl.col("broker") == "RJO")
                # extract "30003" from "RJO - 791 30003"
                .then(
                    pl.col("Account")
                    .str.split(" - ")
                    .list.get(1)
                    .str.split(" ")
                    .list.get(-1)
                )
                # extract "U111111" from "IB - U111111"
                .otherwise(pl.col("Account").str.split(" - ").list.get(1))
                .alias("account"),
            )
        )
        if accounts is not None:
            raw = raw.filter(pl.col("account").is_in(accounts))
        return raw

    def transform(self, raw: pl.DataFrame) -> pl.DataFrame:
        transformed = (
            raw.with_columns(
                pl.when(pl.col("broker") == "RJO")
                .then(pl.col("Symbol"))
                .otherwise(pl.lit(""))
                .str.to_uppercase()
                .alias("bbg_yellow"),
            )
            .group_by("account", "Security")
            .agg(
                pl.col("Quantity").sum().cast(pl.Int64).alias("quantity"),
                pl.col("MP").first().alias("price"),
                pl.col("bbg_yellow").first(),
                pl.col("CCY").first().alias("local_ccy"),
                pl.col("Security").first().alias("description"),
            )
            .select(
                pl.col("account").alias("account_id"),
                pl.col("description"),
                pl.col("bbg_yellow"),
                pl.col("quantity"),
                pl.col("price"),
                pl.col("local_ccy"),
            )
        )
        return transformed


class FormidiumPositionLoader:
    def __init__(self, filepath: str):
        self._filepath = filepath

    def extract(self, date: dt.date, accounts: list[str] | None = None) -> pl.DataFrame:
        broker_names = {
            "RJO' Brien Bank A/c": Broker.RJO,
            "Interactive Brokers": Broker.IB,
        }
        raw = (
            pl.read_excel(self._filepath, read_options={"header_row": 3})
            .filter(pl.col("Symbol").str.len_chars() > 0)
            .with_columns(
                pl.col("Account")
                .str.split(" - ")
                .list.get(0)
                .replace(broker_names)
                .alias("broker"),
            )
            .with_columns(
                pl.when(pl.col("broker") == "RJO")
                # extract "30003" from "RJO - 791 30003"
                .then(
                    pl.col("Account")
                    .str.split(" - ")
                    .list.get(1)
                    .str.split(" ")
                    .list.get(-1)
                )
                # extract "U111111" from "IB - U111111"
                .otherwise(pl.col("Account").str.split(" - ").list.get(1))
                .alias("account"),
            )
        )
        if accounts is not None:
            raw = raw.filter(pl.col("account").is_in(accounts))

        return raw

    def transform(self, raw: pl.DataFrame) -> pl.DataFrame:
        transformed = (
            raw.with_columns(
                pl.when(pl.col("broker") == "RJO")
                .then(pl.col("Symbol"))
                .otherwise(pl.lit(""))
                .str.to_uppercase()
                .alias("bbg_yellow"),
            )
            .group_by("account", "Security")
            .agg(
                pl.col("Quantity").sum().cast(pl.Int64).alias("quantity"),
                pl.col("MP").first().alias("price"),
                pl.col("bbg_yellow").first(),
                pl.col("CCY").first().alias("local_ccy"),
                pl.col("Security").first().alias("description"),
            )
            .select(
                pl.col("account").alias("account_id"),
                pl.col("description"),
                pl.col("bbg_yellow"),
                pl.col("quantity"),
                pl.col("price"),
                pl.col("local_ccy"),
            )
        )

        return transformed
