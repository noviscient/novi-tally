from typing import Iterable

import requests

BASE_URL = "https://api.openfigi.com"
VERSION = "v3"


class OpenFigiApi:
    def __init__(
        self,
        api_key: str,
        version: str = "v3",
        base_url: str = "https://api.openfigi.com",
    ) -> None:
        self._header = {
            "Content-Type": "text/json",
            "X-OPENFIGI-APIKEY": api_key,
        }
        self._url = f"{base_url}/{version}"

    def map_jobs(
        self, jobs: list[dict[str, str]]
    ) -> list[dict[str, list[dict[str, str]]]]:
        response = requests.post(
            url=f"{self._url}/mapping/",
            json=jobs,
            headers=self._header,
        )

        if response.status_code != 200:
            raise Exception("Bad response code {}".format(str(response.status_code)))
        return response.json()

    def get_bbg_mapping_table(self, bb_globals: Iterable[str]) -> dict[str, str]:
        """Get a mapping table from BB globals to BBG yellows."""
        jobs = [
            {"idType": "ID_BB_GLOBAL", "idValue": bb_global} for bb_global in bb_globals
        ]
        job_responses = self.map_jobs(jobs)

        mapping_table = dict()
        for r in job_responses:
            d = r["data"][0]
            if d["marketSector"] == "Equity":
                bbg_yellow = f"{d['ticker']} {d['exchCode']} {d['marketSector']}"
            elif d["securityType"] == "Index Option":
                bbg_yellow = f"{d['securityDescription']} {d['marketSector']}"
            else:
                bbg_yellow = f"{d['ticker']} {d['marketSector']}"
            mapping_table[d["figi"]] = bbg_yellow

        return mapping_table
