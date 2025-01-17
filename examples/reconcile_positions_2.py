"""
# Reconcile positions for external fundboxes (FA data comes from API)

# Dates
Specific dates are used to determine the position at the Fund Admin, Broker and Enfusion,
therefore to clarify the requirement:
Fund Admin : we pass the real evaluation date - so the last date of the Month in question.
Broker/Enfusion : we pass the last business date of the Month in question.
"""

import datetime
from novi_tally import Position
from .utils import get_logger, get_last_bdate

logger = get_logger()


anar_accounts = [
    "U11022080",
    "U11027852",
    "U19728903",
]

alba_accounts = [
    "U15108315",
    "U15188068",
    "U15793786",
]

accounts_to_check = alba_accounts
date_to_check = datetime.date(2024, 11, 30)
last_bdate_to_check = get_last_bdate(date_to_check)
broker_to_check = "ib"
fund_admin_to_check = "formidium"


broker_position = Position.from_config_file(
    provider=broker_to_check,
    date=last_bdate_to_check,
    config_filepath="config.toml",
    accounts=accounts_to_check,
)

logger.log_message(f"### Broker ({broker_to_check}) standardised dataset. ###")
logger.log_message(broker_position.data)

fund_admin_position = Position.from_config_file(
    provider=fund_admin_to_check,
    date=date_to_check,
    config_filepath="config.toml",
    accounts=accounts_to_check,
)

logger.log_message("### Fund Administration standardised dataset. ###")
logger.log_message(fund_admin_position.data)

diff, new_left_only, new_right_only = broker_position.reconcile_with(
    fund_admin_position,
    instrument_identifier="description",
    fallback_identifier="bbg_yellow",
)

logger.log_message(
    "### Differences in name/description, quantity, price & currency between the Broker File and the Fund Administration File. ###"
)
logger.log_message(diff)
logger.log_message(
    f"### Left Dataset - Data in the Broker ({broker_to_check}) dataset - not in the Fund Administrator dataset. ###"
)
logger.log_message(new_left_only)
logger.log_message(
    f"### Right Dataset - Data in the Fund Administrator dataset - not in the Broker ({fund_admin_to_check}) dataset. ###"
)
logger.log_message(new_right_only)
