import datetime
from novi_tally import Position
from .utils import get_logger, get_last_bdate

# Download "PAF" broker/FA/PMS standardized reconcilication data into local directory.

logger = get_logger()

paf_accounts = {
    "rjo": [
        "30012",
        "30014",
        "30015",
        "30016",
    ],
    "ib": [
        "U19923882",
        "U8674826",
    ],
}


date_to_check = datetime.date(2024, 11, 30)
last_bdate_to_check = get_last_bdate(date_to_check)
path = "temp_data"

for broker, broker_accounts in paf_accounts.items():
    if broker not in ("ib", "rjo", "enfusion", "formidium"):
        raise ValueError(f"Invalid broker: {broker}")

    broker_position = Position.from_config_file(
        provider=broker,
        date=last_bdate_to_check,
        config_filepath="config.toml",
        accounts=broker_accounts,
    )

    fund_admin_position = Position.from_config_file(
        provider="formidium",
        # this date should be the real eval date, not the last biz date
        date=date_to_check,
        config_filepath="config.toml",
        accounts=broker_accounts,
    )

    enfusion_position = Position.from_config_file(
        provider="enfusion",
        date=last_bdate_to_check,
        config_filepath="config.toml",
        accounts=broker_accounts,
    )

    diff, left, right = broker_position.reconcile_with(
        fund_admin_position,
        instrument_identifier="description",
        fallback_identifier="bbg_yellow",
    )

    diff.write_csv(f"{path}/{broker}_formidium_comps_diff_{str(date_to_check)}.csv")
    left.write_csv(
        f"{path}/{broker}_formidium_comps_{broker}_only_{str(date_to_check)}.csv"
    )
    right.write_csv(
        f"{path}/{broker}_formidium_comps_formidium_only_{str(date_to_check)}.csv"
    )

    diff, left, right = broker_position.reconcile_with(
        enfusion_position,
        instrument_identifier="description",
        fallback_identifier="bbg_yellow",
    )
    diff.write_csv(f"{path}/{broker}_enfusion_comps_diff_{str(date_to_check)}.csv")
    left.write_csv(
        f"{path}/{broker}_enfusion_comps_{broker}_only_{str(date_to_check)}.csv"
    )
    right.write_csv(
        f"{path}/{broker}_enfusion_comps_enfusion_only_{str(date_to_check)}.csv"
    )
