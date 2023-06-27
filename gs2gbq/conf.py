import os

current_file_path = os.path.abspath(__file__)

# Get the directory containing the current file
current_directory = os.path.dirname(current_file_path)

# Construct the path to the logging.ini file
logging_ini_path = os.path.join(current_directory, "logging.ini")


# CREDENTIAL_FILE = "C:\\Users\\giang\\Downloads\\warm-height-208405-1a84e2764d5f.json"
CREDENTIAL_FILE = os.path.join(current_directory, "google sheet import.json")
JOB_CONFIG_FILE = "https://docs.google.com/spreadsheets/d/1yI6kAO8AHFRoTwlkh28Pyk3rRNxkYGkmz33qGMTdck4/edit?usp=sharing"
# LOG_FILE = "https://docs.google.com/spreadsheets/d/1QU9JJXAXLEaHgz3By90c98ga5khoJni4XXiUuY-W0rA/edit?usp=sharing"
LOG_FILE = "https://docs.google.com/spreadsheets/d/1ukzwx2sYDZdNhYk_bZSZAD41eBWdobf9_7pJXM-_UkE/edit?usp=sharing"
