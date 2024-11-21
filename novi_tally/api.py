import datetime as dt
from functools import cached_property
from typing import Literal

import polars as pl

from novi_tally.config import load_config
from novi_tally.dataloaders import ib, rjo
from novi_tally.errors import ConfigError
from novi_tally.protocols import PositionLoader
from novi_tally.schemas import PositionSchema


class Position:
    DATALOADER_CLASS_MAPPING = {
        "ib": ib.IbPositionLoader,
        "rjo": rjo.RjoPositionLoader,
    }

    def __init__(
        self,
        dataloader: PositionLoader,
        date: dt.date,
        provider_name: str = "custom",
        accounts: list[str] | None = None,
    ):
        self.dataloader = dataloader
        self.date = date
        self.accounts = accounts
        self.provider_name = provider_name

    @classmethod
    def from_config_file(
        cls,
        provider: Literal["ib", "rjo", "enfusion", "formidium"],
        config_filepath: str,
        date: dt.date,
        accounts: list[str] | None = None,
    ):
        config = load_config(config_filepath)

        try:
            dataloader_kwargs = config[provider]
        except KeyError as e:
            raise ConfigError(
                f"No config found for provider {provider} in file {config_filepath}"
            ) from e

        try:
            dataloader_cls = Position.DATALOADER_CLASS_MAPPING[provider]
        except KeyError as e:
            raise KeyError(f"PositionLoader not defined for provider {provider}") from e

        dataloader = dataloader_cls(**dataloader_kwargs)

        return cls(
            dataloader=dataloader, date=date, accounts=accounts, provider_name=provider
        )

    @cached_property
    def data(self) -> pl.DataFrame:
        raw = self.dataloader.extract(date=self.date, accounts=self.accounts)
        transformed = self.dataloader.transform(raw)
        return PositionSchema.validate(transformed)  # type: ignore

    def reconcile_with(
        self,
        other: "Position",
        instrument_identifier: Literal["description", "bbg_yellow"] = "description",
        fallback_identifier: Literal["description", "bbg_yellow"] | None = None,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Compare positions between two different providers and identify discrepancies.

        Args:
            other: Another Position instance to compare against. Must have a different provider_name.
            instrument_identifier: The column to use for matching instruments across providers.
                Can be either "description" or "bbg_yellow". Defaults to "description".

        Returns:
            A tuple of three polars DataFrames:
            - diff: Positions present in both providers but with mismatched values
                   (price, quantity, or currency differences)
            - left_only: Positions present only in this (self) provider
            - right_only: Positions present only in the other provider

        Raises:
            ValueError: If both Position instances have the same provider_name.

        Notes:
            The diff DataFrame includes columns suffixed with provider names and additional
            columns showing the differences (price_diff, quantity_diff) and currency comparison.
        """

        if other.provider_name == self.provider_name:
            raise ValueError("`other` can't have the same `provider_name`")

        # make a copy and add provider name to column names
        l_suffix = self.provider_name
        r_suffix = other.provider_name

        left = self.data.clone().rename(
            lambda name: f"{name}_{l_suffix}"
            # if name not in ["account_id", "bbg_yellow"]
            # else name
        )
        right = other.data.clone().rename(
            lambda name: f"{name}_{r_suffix}"
            # if name not in ["account_id", instrument_identifier]
            # else name
        )

        diff, left_only, right_only = Position._reconcile_dataframes(
            left=left,
            right=right,
            l_suffix=l_suffix,
            r_suffix=r_suffix,
            instrument_identifier=instrument_identifier,
        )

        if not fallback_identifier:
            return diff, left_only, right_only

        # further reconcile unmatched rows with `fallback_identifier`
        new_diff, new_left_only, new_right_only = Position._reconcile_dataframes(
            left=left_only,
            right=right_only,
            l_suffix=l_suffix,
            r_suffix=r_suffix,
            instrument_identifier=fallback_identifier,
        )

        return pl.concat([diff, new_diff]), new_left_only, new_right_only

    @staticmethod
    def _reconcile_dataframes(
        left: pl.DataFrame,
        right: pl.DataFrame,
        l_suffix: str,
        r_suffix: str,
        instrument_identifier: str,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        def _diff_col(col_name: str, l_suffix: str, r_suffix) -> pl.Expr:
            return (
                pl.col(f"{col_name}_{l_suffix}") - pl.col(f"{col_name}_{r_suffix}")
            ).alias(f"{col_name}_diff")

        left = left.filter(pl.col(f"{instrument_identifier}_{l_suffix}").is_not_null())
        right = right.filter(
            pl.col(f"{instrument_identifier}_{r_suffix}").is_not_null()
        )

        diff = (
            left.join(
                other=right,
                left_on=[
                    f"account_id_{l_suffix}",
                    f"{instrument_identifier}_{l_suffix}",
                ],
                right_on=[
                    f"account_id_{r_suffix}",
                    f"{instrument_identifier}_{r_suffix}",
                ],
                how="inner",
                validate="1:1",
                coalesce=False,
            )
            .with_columns(
                _diff_col("price", l_suffix, r_suffix),
                _diff_col("quantity", l_suffix, r_suffix),
                (
                    pl.col(f"local_ccy_{l_suffix}") == pl.col(f"local_ccy_{l_suffix}")
                ).alias("same_ccy?"),
            )
            .filter(
                pl.col("price_diff").abs() > 0,
                pl.col("quantity_diff").abs() > 0,
                ~pl.col("same_ccy?"),
            )
        )

        left_only = left.join(
            other=right,
            left_on=[f"account_id_{l_suffix}", f"{instrument_identifier}_{l_suffix}"],
            right_on=[f"account_id_{r_suffix}", f"{instrument_identifier}_{r_suffix}"],
            how="anti",
        )
        # NOTE: now `right` uses `left_on`, and `left` uses `right_on` :)
        right_only = right.join(
            other=left,
            right_on=[f"account_id_{l_suffix}", f"{instrument_identifier}_{l_suffix}"],
            left_on=[f"account_id_{r_suffix}", f"{instrument_identifier}_{r_suffix}"],
            how="anti",
        )

        return diff, left_only, right_only
