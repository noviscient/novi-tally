import logging
import datetime as dt
import os
import polars as pl

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

        file_handler = logging.FileHandler(self.log_filename)
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
