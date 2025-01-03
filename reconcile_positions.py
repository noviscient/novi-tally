import datetime
from novi_tally import Position
from novi_tally.dataloaders.formidium import FormidiumPositionLoader


# 1. load and transform data from IB
date = datetime.date(2024, 11, 29)
alba_accounts = ["U15108315", "U15188068", "U15793786"]

ib_position = Position.from_config_file(
    provider="ib",
    date=date,
    config_filepath="config.toml",
    accounts=alba_accounts,
)

print(ib_position.data)

# 2. load and transform data from Formidium
form_dataloader = FormidiumPositionLoader(
    filepath="./files/Alba_Capital_Volatility_Strategies_Fund_-_Noviscient_Solutions_VCC_Reporting Package_2024-11-30.xlsx"
)
local_position = Position(
    dataloader=form_dataloader,
    date=date,
    provider_name="local",
)

print(local_position.data)

# 3. reconcile
diff, new_left_only, new_right_only = ib_position.reconcile_with(
    local_position, instrument_identifier="description"
)

print(diff)
print(new_left_only)
print(new_right_only)
