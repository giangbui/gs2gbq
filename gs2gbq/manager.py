import os
from datetime import datetime, date

from typing import Any

import utils
from conf import CREDENTIAL_FILE, JOB_CONFIG_FILE, LOG_FILE
from gshandler import GSHandler
from jobconfig import ConfigLoader, configCheck, PASS, FAIL

import logging.config

LOG_SHEET = "Sheet1"

# Get the path of the current file
current_file_path = os.path.abspath(__file__)

# Get the directory containing the current file
current_directory = os.path.dirname(current_file_path)

# Construct the path to the logging.ini file
logging_ini_path = os.path.join(current_directory, "logging.ini")

# Use the logging.ini path in your logging configuration
logging.config.fileConfig(logging_ini_path)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set the desired log level


class LogWriter:
    def __init__(self, gs_url: str) -> None:
        self.gs_log = GSHandler(gs_url, LOG_SHEET)

    @utils.log_execution
    def write_row(self, row_data):
        self.gs_log.write(row_data)


class JobManager:
    def __init__(self, job_config_url, log_file) -> None:
        self.job_config_handler = ConfigLoader(job_config_url)
        self.log_handler = LogWriter(log_file)

    @utils.log_execution
    def run(self):
        self.log_handler.write_row([""])
        self.log_handler.write_row([f"NEW RUN on {datetime.now()}"])
        self.log_handler.write_row([""])

        config_check_result = self.job_config_handler.sanity_check()
        if config_check_result.outcome == FAIL:
            self.log_handler.write_row(config_check_result.msg)
            return

        cfg = self.job_config_handler.config
        for _, row in cfg.iterrows():
            # Get the current date
            current_date = date.today()

            if not row["schedule"]:
                self.log_handler.write_row(
                        [row["gs"], row["sheet"], row["range"], "NOT SCHEDULED"]
                    )
                continue

            row["schedule"] = row["schedule"].strip()

            if row["schedule"] != "d":
                # if scheduled weekly, only run on Monday
                if row["schedule"] == "w" and current_date.weekday() != 0:
                    self.log_handler.write_row(
                        [row["gs"], row["sheet"], row["range"], "NOT SCHEDULED"]
                    )
                    continue

                # if scheduled monthly, only run on the first date of the month
                if row["schedule"] == "m" and current_date.day != 1:
                    self.log_handler.write_row(
                        [row["gs"], row["sheet"], row["range"], "NOT SCHEDULED"]
                    )
                    continue

                days = [int(d) for d in row["schedule"].split(",")]
                if current_date.day not in days:
                    self.log_handler.write_row(
                        [row["gs"], row["sheet"], row["range"], "NOT SCHEDULED"]
                    )
                    continue

            try:
                gs_handler = GSHandler(row["gs"], row["sheet"], row["range"])
                starting_row = 2
                try:
                    starting_row = int(row["startingrow"])
                except Exception as e:
                    pass

                df = gs_handler.read_data(starting_row)
                gs_handler.push_data_to_big_query(df, row["table"])
                self.log_handler.write_row(
                    [row["gs"], row["sheet"], row["range"], "SUCCESS"]
                )
            except Exception as e:                
                logging.error(f"Line {utils.lineno()}: {str(e)}")
                self.log_handler.write_row(
                    [row["gs"], row["sheet"], row["range"], "FAIL"]
                )


if __name__ == "__main__":
    manager = JobManager(JOB_CONFIG_FILE, LOG_FILE)
    manager.run()
