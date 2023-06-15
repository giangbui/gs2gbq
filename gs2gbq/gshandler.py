import os
import pandas as pd
import gspread
from google.cloud import bigquery
from google.oauth2 import service_account

import utils
from conf import CREDENTIAL_FILE

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


class GSHandler:
    def __init__(self, gs_url: str, sheet: str, range:str=None) -> None:
        self.url = gs_url
        self.sheet_name = sheet
        self.sheet_range = range
        self.credential_file = CREDENTIAL_FILE

    @utils.timing_decorator
    @utils.log_execution
    def read_data(self):
        def _helper(column):
            column = column.strip()
            for c in ["/", " ", ":", ";", "-", "!", "?", "\\"]:
                column = column.replace(c, "_")
            return column

        gc = gspread.service_account(filename=self.credential_file)
        sh = gc.open_by_url(self.url)
        try:
            data = sh.worksheet(f"{self.sheet_name}").get(self.sheet_range)
        except gspread.exceptions.APIError as e:
            logging.error(
                f"Can not open the worksheet {self.sheet_name} with range of {self.sheet_range} in {self.url}"
            )
            raise
        except Exception as e:
            logging.error(
                f"Can not open the worksheet {self.sheet_name} with range of {self.sheet_range} in {self.url}"
            )
            raise

        df = pd.DataFrame(data)
        headers = df.iloc[0]

        df = df[1:]
        df.columns = [_helper(h) for h in headers]

        return df

    @utils.log_execution
    def write(self, data=None):
        gc = gspread.service_account(filename=self.credential_file)
        sh = gc.open_by_url(self.url)
        try:
            ws = sh.worksheet(f"{self.sheet_name}")
            ws.append_row(data)
        except gspread.exceptions.APIError as e:
            logging.error(
                f"Can not write logs to the worksheet {self.url} in {sheet_name}"
            )

    @utils.timing_decorator
    @utils.log_execution
    def push_data_to_big_query(self, sheet_df, table_name):
        credentials = service_account.Credentials.from_service_account_file(
            self.credential_file,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        try:
            client = bigquery.Client(
                credentials=credentials, project=credentials.project_id
            )
            job_config = bigquery.LoadJobConfig()
            job_config.autodetect = True
            job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ]

            # Delete existing table and re-create. Idiot approach!
            try:
                client.delete_table(f"{credentials.project_id}.{table_name}")
            except Exception as e:
                # Silently delete table if exist
                pass

            load_job = client.load_table_from_dataframe(
                sheet_df, table_name, job_config=job_config
            )
            logging.info(f"Starting job {load_job.job_id} to ingest {self.url}")
            load_job.result()
            logging.info("Job finished.")

        except Exception as e:
            logging.error(
                f"An error occurred during data ingestion: {sheet_df} to {table_name}. Detail {str(e)}"
            )
            raise

        finally:
            client = None
