"""
# Download "PAF" broker/FA/PMS standardised reconcilication data into local directory.

# Dates
Specific dates are used to determine the position at the Fund Admin, Broker and Enfusion, 
therefore to clarify the requirement:
Fund Admin : we pass the real evaluation date - so the last date of the Month in question.
Broker/Enfusion : we pass the last business date of the Month in question.
"""

import datetime
from novi_tally import Position
from .utils import get_logger, get_last_bdate
from novi_tally.dataloaders.formidium import FormidiumPositionLoader

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


# Format the datetime object in HHMM_DDMMMYYYY format
now = datetime.datetime.now()
formatted_datetime = now.strftime("%H%M-%d%b%Y")

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

    enfusion_position = Position.from_config_file(
        provider="enfusion",
        date=last_bdate_to_check,
        config_filepath="config.toml",
        accounts=broker_accounts,
    )

    #    fund_admin_position = Position.from_config_file(
    #        provider="formidium",
    #        # this date should be the real eval date, not the last biz date
    #        date=date_to_check,
    #        config_filepath="config.toml",
    #        accounts=broker_accounts,
    #    )

    nav_report = "./files/" + "PAF_2024-11-30.xlsx"
    form_dataloader = FormidiumPositionLoader(filepath=nav_report)
    fund_admin_position = Position(
        dataloader=form_dataloader,
        date=date_to_check,
        provider_name="local",
        accounts=broker_accounts,
    )

    ##### Broker versus Fund Admin #####
    print("Broker versus Fund Admin ")
    diff, left, right = broker_position.reconcile_with(
        fund_admin_position,
        instrument_identifier="description",
        fallback_identifier="bbg_yellow",
    )

    diff_path = f"{path}/{formatted_datetime}_{broker}_formidium_comps_diff_{str(date_to_check)}.csv"
    print("Diff Path [" + diff_path + "]")
    diff.write_csv(diff_path)

    left_path = f"{path}/{formatted_datetime}_{broker}_formidium_comps_{broker}_only_{str(date_to_check)}.csv"
    print("Left Path [" + left_path + "]")
    left.write_csv(left_path)

    right_path = f"{path}/{formatted_datetime}_{broker}_formidium_comps_formidium_only_{str(date_to_check)}.csv"
    print("Right Path [" + right_path + "]")
    right.write_csv(right_path)

    #####  Broker versus Enfusion #####
    print("Broker versus Enfusion")
    diff, left, right = broker_position.reconcile_with(
        enfusion_position,
        instrument_identifier="description",
        fallback_identifier="bbg_yellow",
    )

    diff_path = f"{path}/{formatted_datetime}_{broker}_enfusion_comps_diff_{str(date_to_check)}.csv"
    print("Diff Path [" + diff_path + "]")
    diff.write_csv(diff_path)

    left_path = f"{path}/{formatted_datetime}_{broker}_enfusion_comps_{broker}_only_{str(date_to_check)}.csv"
    print("Left Path [" + left_path + "]")
    left.write_csv(left_path)

    right_path = f"{path}/{formatted_datetime}_{broker}_enfusion_comps_enfusion_only_{str(date_to_check)}.csv"
    print("Right Path [" + right_path + "]")
    right.write_csv(right_path)
