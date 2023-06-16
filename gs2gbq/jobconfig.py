import os

import numpy as np
import pandas as pd

import utils
from gshandler import GSHandler
from conf import CREDENTIAL_FILE, JOB_CONFIG_FILE

import logging.config

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

PASS = True
FAIL = False

JOB_CONFIG_SHEET = "jobs"
DEFAULT_CONFIG_RANGE = "A:H"


class configCheck:
    def __init__(self, outcome, msg) -> None:
        self.outcome = outcome
        self.msg = msg


class ConfigLoader:
    @utils.log_execution
    def __init__(
        self,
        config_file_path: str,
        sheet: str = JOB_CONFIG_SHEET,
        range: str = DEFAULT_CONFIG_RANGE,
    ) -> None:
        handler = GSHandler(config_file_path, sheet, range)
        self.config = handler.read_data()

    def sanity_check(self):
        """Check if the config file is in the right format"""
        msg = []
        if self.config.empty:
            msg.append("Can not load the config file")
            return configCheck(FAIL, msg)

        if set(self.config.columns) != {
            "gs",
            "table",
            "job_id",
            "range",
            "schedule",
            "job_name",
            "sheet",
            "startingrow"
        }:
            msg.append(
                "The config file should contain gs, table, job_id, range, schedule, job_name, sheet, startingrow as columns"
            )
            return configCheck(FAIL, msg)

        return configCheck(PASS, msg)
