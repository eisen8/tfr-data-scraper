from datetime import datetime


def tprint(message):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}")


def tprintln():
    print("")
