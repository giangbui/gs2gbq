import functools
import time

import smtplib
import traceback
from email.mime.text import MIMEText

import logging.config

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set the desired log level


def get_function_name(func):
    return f"{func.__qualname__.split('.')[-2]}.{func.__name__}"


def log_execution(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logging.info(f"Executing {get_function_name(func)}")
        result = func(*args, **kwargs)
        logging.info(f"Finished executing {get_function_name(func)}")
        return result

    return wrapper


def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logging.info(
            f"Function {get_function_name(func)} took {end_time - start_time} seconds to run."
        )
        return result

    return wrapper


def email_on_failure(sender_email, password, recipient_email):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # format the error message and traceback
                err_msg = f"Error: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"

                # create the email message
                message = MIMEText(err_msg)
                message["Subject"] = f"{func.__name__} failed"
                message["From"] = sender_email
                message["To"] = recipient_email

                # send the email
                with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
                    smtp.login(sender_email, password)
                    smtp.sendmail(sender_email, recipient_email, message.as_string())

                # re-raise the exception
                raise

        return wrapper

    return decorator
