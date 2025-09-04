import logging
from common.dir import address_log
def setup_logging(
    log_file = address_log,
    level=logging.INFO,
    log_format='%(asctime)s - %(levelname)s - %(message)s'
):
    logging.basicConfig(
        filename=log_file,
        level=level,
        format=log_format,
        encoding='utf-8'
    )