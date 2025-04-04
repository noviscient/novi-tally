import datetime

from .utils import get_last_bdate
from novi_tally import Margin


paf_accounts = {
    "rjo": [
        "30012",
        "30014",
        "30015",
        "30016",
    ],
    "ib": [
        "U6016819",
    ],
}


date_to_check = datetime.date(2025, 1, 31)
last_bdate_to_check = get_last_bdate(date_to_check)

rjo_margin = Margin.from_config_file(
    provider="rjo",
    date=last_bdate_to_check,
    config_filepath="config.toml",
    accounts=paf_accounts["rjo"],
)

print(rjo_margin.data)

ib_margin = Margin.from_config_file(
    provider="ib",
    date=last_bdate_to_check,
    config_filepath="config.toml",
    accounts=paf_accounts["ib"],
)

print(ib_margin.data)
