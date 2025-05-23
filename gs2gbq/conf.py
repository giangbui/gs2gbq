import os
from pydantic_settings import BaseSettings

current_file_path = os.path.abspath(__file__)

# Get the directory containing the current file
current_directory = os.path.dirname(current_file_path)

# Construct the path to the logging.ini file
logging_ini_path = os.path.join(current_directory, "logging.ini")

# test gs
# JOB_CONFIG_FILE = "https://docs.google.com/spreadsheets/d/1yI6kAO8AHFRoTwlkh28Pyk3rRNxkYGkmz33qGMTdck4/edit?usp=sharing"
# LOG_FILE = "https://docs.google.com/spreadsheets/d/1QU9JJXAXLEaHgz3By90c98ga5khoJni4XXiUuY-W0rA/edit?usp=sharing"
# LOG_FILE = "https://docs.google.com/spreadsheets/d/1ukzwx2sYDZdNhYk_bZSZAD41eBWdobf9_7pJXM-_UkE/edit?usp=sharing"

# radar gs
# JOB_CONFIG_FILE = "https://docs.google.com/spreadsheets/d/1vG1SpShp8MnAZ3RW8dqi4hEwJtSCFoe7GgB1kV6x2ms/edit?usp=sharing"
# LOG_FILE = "https://docs.google.com/spreadsheets/d/1QU9JJXAXLEaHgz3By90c98ga5khoJni4XXiUuY-W0rA/edit?usp=sharing"

class Settings(BaseSettings):
    # default values: radar
    CREDENTIAL_FILE: str = os.path.join(current_directory, "google sheet import.json")
    JOB_CONFIG_FILE: str = "https://docs.google.com/spreadsheets/d/1vG1SpShp8MnAZ3RW8dqi4hEwJtSCFoe7GgB1kV6x2ms/edit?usp=sharing"
    LOG_FILE: str = "https://docs.google.com/spreadsheets/d/1QU9JJXAXLEaHgz3By90c98ga5khoJni4XXiUuY-W0rA/edit?usp=sharing"

    class Config:
        env_file = f"{current_directory}/.env"



settings = Settings()
print(settings.model_dump())