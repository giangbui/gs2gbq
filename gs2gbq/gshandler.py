import os
import pandas as pd
import gspread
import time
import random

import backoff

import google
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
    def __init__(self, gs_url: str, sheet: str, ranges: str = None) -> None:
        self.url = gs_url
        self.sheet_name = sheet
        self.sheet_ranges = [r.strip() for r in ranges.split(",")] if ranges else [None]
        self.credential_file = CREDENTIAL_FILE

    @utils.timing_decorator
    @utils.log_execution
    @backoff.on_exception(backoff.expo, gspread.exceptions.APIError, max_tries=8, on_backoff=utils.backoff_hdlr, logger='logger')
    def read_data(self, starting_row: str=2):
        def _helper(column):
            column = column.strip()
            for c in ["/", " ", ":", ";", "-", "!", "?", "\\", "(", ")", "$", "^", "&", "*", "+", "#", "%", ".","'", "`", "~", "="]:
                column = column.replace(c, "_")
            return column

        gc = gspread.service_account(filename=self.credential_file)
        sh = gc.open_by_url(self.url)
                
        df = pd.DataFrame()
        for range in self.sheet_ranges:            
            if ":" not in range:
                range = f"{range}:{range}"
            # data = get_worksheet_data(sh, self.sheet_name, range)         
            data = sh.worksheet(f"{self.sheet_name}").get(range)  
            # data = sh.worksheet(f"{self.sheet_name}").get(range, value_render_option='UNFORMATTED_VALUE')
            df = pd.concat([df, pd.DataFrame(data)], axis=1)
        
        headers = df.iloc[0]
        df = df[starting_row-1:]
        df.columns = [_helper(h) for h in headers]

        return df

    @utils.log_execution
    @backoff.on_exception(backoff.expo, gspread.exceptions.APIError, max_tries=9, on_backoff=utils.backoff_hdlr, logger="logger")
    def write(self, data=None):
        gc = gspread.service_account(filename=self.credential_file)   
        sh = gc.open_by_url(self.url)
        ws = sh.worksheet(f"{self.sheet_name}")
        ws.append_row(data)
            
    @utils.timing_decorator
    @utils.log_execution
    @backoff.on_exception(backoff.expo, google.api_core.exceptions.GoogleAPICallError, max_tries=8, on_backoff=utils.backoff_hdlr, logger="logger")
    def push_data_to_big_query(self, sheet_df, table_name, schema=None):
        credentials = service_account.Credentials.from_service_account_file(
            self.credential_file,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )
        # job_config = bigquery.LoadJobConfig()
        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
            schema=schema
        )
                
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

        start =  0
        offset = 1000
        while start < sheet_df.shape[0]:
            load_job = client.load_table_from_dataframe(
                sheet_df.iloc[start:start+offset,:], table_name, job_config=job_config
            )        
            load_job.result()
            start = start + offset
            time.sleep(5)
        logging.info("Job finished.")

