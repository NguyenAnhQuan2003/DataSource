from dataclasses import dataclass
from pymongo import MongoClient
from pymongo.collection import Collection
from common.logging_config import setup_logging
from typing import Dict, List
setup_logging()


@dataclass
class MongoConfig:
    uri: str
    db_name: str

def get_mongo_client(cfg: MongoConfig) -> MongoClient:
    return MongoClient(cfg.uri)


def get_collection(db, name: str) -> Collection:
    return db[name]

def project_fields(fields: List[str]) -> Dict:
    return {f: 1 for f in fields}