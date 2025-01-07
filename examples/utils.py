import logging
import datetime as dt


class Logger:
    def __init__(self):
        self.current_time = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_filename = f"logs/log_{self.current_time}.txt"

    def create(self):
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        logging.basicConfig(
            filename=self.log_filename,
            level=logging.INFO,
            format="%(message)s",
        )

    def log_message(self, message):
        logging.info(str(message))


def get_logger():
    logger = Logger()
    logger.create()
    return logger
