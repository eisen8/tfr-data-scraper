import sys
import traceback
from datetime import datetime


class Logger:
    """
    A simple logger class that prints log messages with timestamps.
    """

    _RED = "\033[91m"
    _RESET = "\033[0m"
    _ERROR_MESSAGES = []

    num_errors = 0

    @staticmethod
    def _log(level, message, color=""):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{color}[{timestamp}] [{level}] - {message}{Logger._RESET}")

    @staticmethod
    def info(message):
        Logger._log("INFO", message)

    @staticmethod
    def error(message, exception=None):
        Logger._log("ERROR", message, Logger._RED)
        if exception:
            print(Logger._RED + "".join(traceback.format_exception(None, exception, exception.__traceback__)) + Logger._RESET)

        Logger._ERROR_MESSAGES.append((message, exception))
        Logger.num_errors += 1

    @staticmethod
    def print_error_messages():
        for message in Logger._ERROR_MESSAGES:
            Logger._log("ERROR", message[0], Logger._RED)
            exception = message[1]
            if exception:
                print(Logger._RED + "".join(traceback.format_exception(None, exception, exception.__traceback__)) + Logger._RESET)
