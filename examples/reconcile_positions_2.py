import pandas
import datetime
from novi_tally import Position


paf_accounts = [
    "U19923882",
    "U8674826",
    "30012",
    "30014",
    "30015",
    "30016",
]

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


def get_last_bdate(date: datetime.date):
    if date.weekday() < 5:
        return date
    return (
        date - datetime.timedelta(days=date.weekday() - 4)
        if date.weekday() == 5
        else date - datetime.timedelta(days=1)
    )


accounts_to_check = anar_accounts
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

fund_admin_position = Position.from_config_file(
    provider=fund_admin_to_check,
    date=last_bdate_to_check,
    config_filepath="config.toml",
    accounts=accounts_to_check,
)

diff, left, right = broker_position.reconcile_with(fund_admin_position)

print(diff)
print(left)
print(right)
