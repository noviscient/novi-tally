"""
# Reconcile positions for external fundboxes (FA data comes from ShareFile)

# Dates
Specific dates are used to determine the position at the Fund Admin, Broker and Enfusion, 
therefore to clarify the requirement:
Fund Admin : we pass the real evaluation date - so the last date of the Month in question.
Broker/Enfusion : we pass the last business date of the Month in question.
"""

import datetime
from novi_tally import Position
from novi_tally.dataloaders.formidium import FormidiumPositionLoader
from .utils import get_logger

logger = get_logger()

# Some consts we need
# 1. Format the datetime object in HHMM_DDMMMYYYY format
now = datetime.datetime.now()
formatted_datetime = now.strftime("%H%M-%d%b%Y")
# 2. Path for output files
path = f"./temp_data"

# 1. Load and transform data from IB
date = datetime.date(2024, 12, 31)


alba_accounts = ["U15108315", "U15188068", "U15793786"]
anar_accounts = ["U11022080", "U11027852", "U19728903"]
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

paf_rjo_accounts = paf_accounts["rjo"]

#### Set up the parameters to use for the run.
accounts_to_use = anar_accounts
broker = "ib"
date = datetime.date(2024, 12, 31)
date_to_check = str(date)

ib_position = Position.from_config_file(
    provider=broker,
    date=date,
    config_filepath="config.toml",
    accounts=accounts_to_use,
)

logger.log_message(f"### Broker ({broker}) standardised dataset. ###")
logger.log_message(ib_position.data)
write_path = f"{path}/{formatted_datetime}_{broker}_standardised_dataset_{str(date_to_check)}.csv"
print(write_path)
ib_position.data.write_csv(write_path)


# 2. Load and transform data from Formidium
# nav_report = "./files/" + "PAF_2024-11-30.xlsx"

nav_report = "./files/" + f"Anar_{date_to_check}.xlsx"
form_dataloader = FormidiumPositionLoader(filepath=nav_report)
local_position = Position(
    dataloader=form_dataloader,
    date=date,
    provider_name="local",
    accounts=accounts_to_use,
)

logger.log_message("### Fund Administration standardised dataset. ###")
logger.log_message(local_position.data)
write_path = (
    f"{path}/{formatted_datetime}_fa_xlsx_standardised_dataset_{str(date_to_check)}.csv"
)
print(write_path)
local_position.data.write_csv(write_path)

# 3. Reconcile
diff, new_left_only, new_right_only = ib_position.reconcile_with(
    local_position,
    instrument_identifier="description",
    fallback_identifier="bbg_yellow",
)

# 1- Differences in name/description, quantity, price & currency between the Broker File and the Fund Administration File.
write_path = f"{path}/{formatted_datetime}_{broker}_formidium_xlsx_comps_diff_{str(date_to_check)}.csv"
print(write_path)
diff.write_csv(write_path)
logger.log_message(
    "### Differences in name/description, quantity, price & currency between the Broker File and the Fund Administration File. ###"
)
logger.log_message(diff)

# 2-Left Dataset - Data in the Broker dataset - not in the Fund Administrator dataset.
write_path = f"{path}/{formatted_datetime}_{broker}_formidium_xlsx_comps_Broker_Only_{str(date_to_check)}.csv"
print(write_path)
new_left_only.write_csv(write_path)
logger.log_message(
    f"### Left Dataset - Data in the Broker ({broker}) dataset - not in the Fund Administrator dataset. ###"
)
logger.log_message(new_left_only)

# 2- Right Dataset - Data in the Fund Administrator dataset - not in the Broker dataset.
write_path = f"{path}/{formatted_datetime}_{broker}_formidium_xlsx_comps_FundAdmin_xlsx_Only_{str(date_to_check)}.csv"
print(write_path)
new_right_only.write_csv(write_path)
logger.log_message(
    f"### Right Dataset - Data in the Fund Administrator dataset - not in the Broker ({broker}) dataset. ###"
)
logger.log_message(new_right_only)
