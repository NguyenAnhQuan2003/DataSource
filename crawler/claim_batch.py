import logging

import requests
from bs4 import BeautifulSoup
from common.logging_config import setup_logging
from common.dir import BATCH_SIZE
setup_logging()

def crawl_product_name(url):
    try:
        resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        title = soup.find("h1")
        if title:
            return title.get_text(strip=True)
    except Exception as e:
        logging.error("Crawling error", e)
    return None

def claim_batch(col, size=BATCH_SIZE):
    docs = list(col.find({"status": "pending"})
                   .limit(size))
    ids = [d["_id"] for d in docs]
    if ids:
        col.update_many({"_id": {"$in": ids}}, {"$set": {"status": "processing"}})
    return docs

