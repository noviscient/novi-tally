import datetime
from novi_tally import Position
from novi_tally.dataloaders.formidium import FormidiumPositionLoader
from .utils import get_logger

# Reconcile positions for external fundboxes (FA data comes from ShareFile)


logger = get_logger()


# 1. Load and transform data from IB
date = datetime.date(2024, 11, 29)
alba_accounts = ["U15108315", "U15188068", "U15793786"]
anar_accounts = ["U11022080", "U11027852", "U19728903"]

accounts_to_use = anar_accounts
provider_to_use = "ib"

ib_position = Position.from_config_file(
    provider=provider_to_use,
    date=date,
    config_filepath="config.toml",
    accounts=accounts_to_use,
)

logger.log_message(f"### Broker ({provider_to_use}) standardised dataset. ###")
logger.log_message(ib_position.data)

# 2. Load and transform data from Formidium
nav_report = "./files/" + "Anar_2024-11-30.xlsx"
form_dataloader = FormidiumPositionLoader(filepath=nav_report)
local_position = Position(
    dataloader=form_dataloader,
    date=date,
    provider_name="local",
)

logger.log_message("### Fund Administration standardised dataset. ###")
logger.log_message(local_position.data)

# 3. Reconcile
diff, new_left_only, new_right_only = ib_position.reconcile_with(
    local_position,
    instrument_identifier="description",
)

logger.log_message(
    "### Differences in name/description, quantity, price & currency between the Broker File and the Fund Administration File. ###"
)
logger.log_message(diff)
logger.log_message(
    f"### Left Dataset - Data in the Broker ({provider_to_use}) dataset - not in the Fund Administrator dataset. ###"
)
logger.log_message(new_left_only)
logger.log_message(
    f"### Right Dataset - Data in the Fund Administrator dataset - not in the Broker ({provider_to_use}) dataset. ###"
)
logger.log_message(new_right_only)
