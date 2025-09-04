from common.connect import MongoConfig
from common.dir import uri, DB_NAME
from crawler.crawl_products import worker

if __name__ == "__main__":
    cfg = MongoConfig(
        uri=uri,
        db_name=DB_NAME
    )
    worker(cfg)