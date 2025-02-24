import datetime as dt
from formidium import Api


class FormidiumApi:
    def __init__(
        self,
        api_key: str,
        passphrase: str,
        api_secret: str,
        base_url: str = "https://api.formidium.com",
    ) -> None:
        self.api = Api(
            api_key=api_key,
            passphrase=passphrase,
            api_secret=api_secret,
            base_url=base_url,
        )

    def read_positions(self, date: dt.date, fund_name: str) -> dict:
        data = self.api.positions(
            fund_names=[fund_name],
            date=date,
        )
        return data
