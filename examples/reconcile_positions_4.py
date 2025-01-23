"""
# Download the Sub-Fund broker/FA/PMS standardised data into local directory.
# No comparisons.

# Dates
Specific dates are used to determine the position at the Fund Admin, Broker and Enfusion,
therefore to clarify the requirement:
Fund Admin : we pass the real evaluation date - so the last date of the Month in question.
Broker/Enfusion : we pass the last business date of the Month in question.
"""

import datetime
from novi_tally import Position
from .utils import get_last_bdate, save_data_to_csv

# TODO - Read this information from the configuration file.
# TODO - Read this information from the configuration file.
subfund_accounts_PAF = {
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

subfund_accounts_ANAR = {
    "ib": [
        "U11022080",
        "U11027852",
        "U19728903",
    ]
}

subfund_accounts_ALBA = {
    "ib": [
        "U15108315",
        "U15188068",
        "U15793786",
    ]
}

# 1: Get a timestamp for logging purposes
# Format the datetime object in HHMM_DDMMMYYYY format
now = datetime.datetime.now()
formatted_datetime = now.strftime("%H%M-%d%b%Y")

# 2: Select Sub Fund to look at
# chk_accounts = paf_accounts
# chk_accounts = anar_accounts
# TODO - Read this information from the configuration file.
chk_accounts = subfund_accounts_ANAR

# 3: Last trading day of the month
# TODO - Read this information from the configuration file.
date_to_check = datetime.date(2024, 12, 31)

# 4: Last day of the month
last_bdate_to_check = get_last_bdate(date_to_check)
print(
    f"Date we are checking: [{str(date_to_check)}]; Last day of the month: [{str(last_bdate_to_check)}] "
)

# 5: Path for our output files
path = "temp_data"

for broker, broker_accounts in chk_accounts.items():
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
        date=date_to_check,
        config_filepath="config.toml",
        accounts=broker_accounts,
    )

    save_data_to_csv(
        broker_position,
        rawdata_filepath=raw_broker_path,
        stddata_filepath=std_broker_path,
    )
    save_data_to_csv(
        enfusion_position,
        rawdata_filepath=raw_enfusion_path,
        stddata_filepath=std_enfusion_path,
    )
    save_data_to_csv(
        fund_admin_position,
        rawdata_filepath=raw_fa_path,
        stddata_filepath=std_fa_path,
    )
