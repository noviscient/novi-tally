"""
# Download the "PAF" broker/FA/PMS standardised data into local directory.
# No comparisons.

# Dates
Specific dates are used to determine the position at the Fund Admin, Broker and Enfusion, 
therefore to clarify the requirement:
Fund Admin : we pass the real evaluation date - so the last date of the Month in question.
Broker/Enfusion : we pass the last business date of the Month in question.
"""

import datetime
from novi_tally import Position
from .utils import get_last_bdate


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

# Format the datetime object in HHMM_DDMMMYYYY format
now = datetime.datetime.now()
formatted_datetime = now.strftime("%H%M-%d%b%Y")

date_to_check = datetime.date(2024, 11, 30)
last_bdate_to_check = get_last_bdate(date_to_check)
path = "temp_data"

for broker, broker_accounts in paf_accounts.items():
    if broker not in ("ib", "rjo", "enfusion", "formidium"):
        raise ValueError(f"Invalid broker: {broker}")

    raw_broker_path = (
        f"{path}/Raw_{formatted_datetime}_{broker}_Broker_{str(date_to_check)}.csv"
    )
    raw_enfusion_path = (
        f"{path}/Raw_{formatted_datetime}_{broker}_Enfusion_{str(date_to_check)}.csv"
    )
    raw_fa_path = (
        f"{path}/Raw_{formatted_datetime}_{broker}_fa_{str(last_bdate_to_check)}.csv"
    )
    std_broker_path = f"{path}/Standardised_{formatted_datetime}_{broker}_Broker_{str(date_to_check)}.csv"
    std_enfusion_path = f"{path}/Standardised_{formatted_datetime}_{broker}_Enfusion_{str(date_to_check)}.csv"
    std_fa_path = f"{path}/Standardised_{formatted_datetime}_{broker}_fa_{str(last_bdate_to_check)}.csv"

    broker_position = Position.from_config_file(
        provider=broker,
        date=last_bdate_to_check,
        config_filepath="config.toml",
        rawdata_file=raw_broker_path,
        accounts=broker_accounts,
    )

    enfusion_position = Position.from_config_file(
        provider="enfusion",
        date=last_bdate_to_check,
        config_filepath="config.toml",
        rawdata_file=raw_enfusion_path,
        accounts=broker_accounts,
    )

    fund_admin_position = Position.from_config_file(
        provider="formidium",
        date=date_to_check,
        config_filepath="config.toml",
        rawdata_file=raw_fa_path,
        accounts=broker_accounts,
    )

    print("Broker Path [" + std_broker_path + "]")
    broker_position.data.write_csv(std_broker_path)

    print("Enfusion Path [" + std_enfusion_path + "]")
    enfusion_position.data.write_csv(std_enfusion_path)

    print("Fund Admin Path [" + std_fa_path + "]")
    fund_admin_position.data.write_csv(std_fa_path)
