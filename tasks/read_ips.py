import logging
from config.logging_config import setup_logging

setup_logging()

def unique_read_ips(db):
    if db is None:
        logging.error("Không đọc được collection!")
        return []
    try:
        return []
    except Exception as e:
        logging.error(e)
        raise