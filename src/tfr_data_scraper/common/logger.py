import sys
import traceback
from datetime import datetime


class Logger:
    """
    A simple logger class that prints log messages with timestamps.
    """

    _RED = "\033[91m"
    _RESET = "\033[0m"

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
