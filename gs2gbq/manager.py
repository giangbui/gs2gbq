import os
from datetime import datetime, date
import time

from google.cloud import bigquery
from typing import Any
import pandas as pd

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
                # self.log_handler.write_row(
                #         [row["gs"], row["sheet"], row["range"], "NOT SCHEDULED"]
                #     )
                continue

            row["schedule"] = row["schedule"].strip().replace(" ", "")

            scheduled_to_run_today = False
            if row["schedule"] == "d":
                scheduled_to_run_today = True
            elif any(day.lower() in row["schedule"].lower().split(",") for day in ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]):
                wd = current_date.weekday()
                dic_mapping = {0: "Mo", 1: "Tu", 2: "We", 3: "Th", 4: "Fr", 5: "Sa", 6: "Su"}
                if dic_mapping[wd].lower() in row["schedule"].lower().split(","):
                    scheduled_to_run_today = True
            elif row["schedule"] == "m" and current_date.day == 1:
                scheduled_to_run_today = True
            else:
                try:
                    days = [int(d) for d in row["schedule"].split(",")]                    
                except Exception:
                    days = []                
                if current_date.day in days:  
                    scheduled_to_run_today = True

            if not scheduled_to_run_today:
                continue

            logging.info("====================================================================================================================")
            logging.info(f"Starting jobid {row.job_id} to ingest {row.gs}")

            try:
                start_time = time.time()
                gs_handler = GSHandler(row["gs"], row["sheet"], row["range"])
                starting_row = 2
                try:
                    starting_row = int(row["startingrow"])
                except Exception as e:
                    pass

                df = gs_handler.read_data(starting_row)
                # Convert numeric columns to appropriate data types
                schema = []
                for col in df.columns:
                    try:
                        # Try to convert to numeric
                        df[col] = pd.to_numeric(df[col])
                    except Exception:
                        # If not numeric, try to convert to datetime
                        try:
                            #df[col] = pd.to_datetime(df[col]).apply(lambda x: x.date())
                            df[col] = pd.to_date(df[col])
                        except Exception:                            
                            schema.append(bigquery.SchemaField( col,bigquery.enums.SqlTypeNames.STRING))
                        
                gs_handler.push_data_to_big_query(df, row["table"], schema)

                end_time = time.time()
                
                self.log_handler.write_row(
                    [row["job_name"], row["gs"], row["sheet"], row["range"], "SUCCESS", f"{end_time - start_time}"]
                )
                logging.info(f"Took {end_time - start_time} seconds to ingest {row['gs']}")
            except Exception as e:                
                logging.error(f"Line {utils.lineno()}: {str(e)}")
                self.log_handler.write_row(
                    [row["job_name"], row["gs"], row["sheet"], row["range"], "FAIL"]
                )


if __name__ == "__main__":
    manager = JobManager(JOB_CONFIG_FILE, LOG_FILE)
    manager.run()
