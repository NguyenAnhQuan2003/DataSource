import pymongo
import logging
from config.logging_config import setup_logging
from config.dir import uri, DB_NAME
setup_logging()


def connect_mongodb():
    try:
        client = pymongo.MongoClient(uri)
        db = client[DB_NAME]
        logging.info("Connected to MongoDB")
        return db
    except Exception as error:
        logging.error(error)
