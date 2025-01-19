import logging
import datetime as dt
import os
import polars as pl
from novi_tally import Position

pl.Config.set_tbl_rows(1000)


class Logger:
    def __init__(self):
        self.current_time = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_filename = f"logs/log_{self.current_time}.txt"

        if not os.path.exists("logs"):
            os.makedirs("logs")

        self.logger = logging.getLogger(__name__)

    def create(self):
        logging.getLogger().handlers.clear()

        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        file_handler = logging.FileHandler(self.log_filename, encoding="utf-8")
        formatter = logging.Formatter("%(message)s\n")

        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        self.logger.setLevel(logging.INFO)

    def log_message(self, message):
        self.logger.info(str(message))


def get_logger():
    logger = Logger()
    logger.create()
    return logger


def get_last_bdate(date: dt.date):
    if date.weekday() < 5:
        return date
    return (
        date - dt.timedelta(days=date.weekday() - 4)
        if date.weekday() == 5
        else date - dt.timedelta(days=1)
    )


def save_data_to_csv(position: Position, rawdata_filepath: str, stddata_filepath: str):
    """
    Extracts raw data from the given position and writes it to a CSV file.
    """
    raw_data = position.dataloader.extract(
        date=position.date, accounts=position.accounts
    )
    print(f"Writing [{position.provider_name}] raw data to {rawdata_filepath}")
    raw_data.write_csv(rawdata_filepath)

    print(f"Writing [{position.provider_name}] std data to {stddata_filepath}")
    position.data.write_csv(stddata_filepath)
