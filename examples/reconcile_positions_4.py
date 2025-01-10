import datetime
from novi_tally import Position
from .utils import get_last_bdate

# Download "PAF" broker/FA/PMS standardized data into local directory.

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
last_bdate_to_check = date_to_check
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

    enfusion_position = Position.from_config_file(
        provider="enfusion",
        date=last_bdate_to_check,
        config_filepath="config.toml",
        accounts=broker_accounts,
    )

    fund_admin_position = Position.from_config_file(
        provider="formidium",
        date=last_bdate_to_check,
        config_filepath="config.toml",
        accounts=broker_accounts,
    )

    broker_position.data.write_csv(f"{path}/{str(date_to_check)}_{broker}_broker.csv")
    enfusion_position.data.write_csv(
        f"{path}/{str(date_to_check)}_{broker}_enfusion.csv"
    )
    fund_admin_position.data.write_csv(
        f"{path}/{str(last_bdate_to_check)}_{broker}_fa.csv"
    )
