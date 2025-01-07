import logging
import datetime as dt


class Logger:
    def __init__(self):
        self.current_time = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_filename = f"logs/log_{self.current_time}.txt"
        self.logger = logging.getLogger(__name__)  # Named logger

    def create(self):
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        file_handler = logging.FileHandler(self.log_filename)
        formatter = logging.Formatter("%(message)s\n")  # Only log message
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
