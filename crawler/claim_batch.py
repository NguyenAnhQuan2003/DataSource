import logging
import requests
from datetime import datetime
from pymongo import ReturnDocument
from bs4 import BeautifulSoup
from common.logging_config import setup_logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from common.dir import BATCH_SIZE
setup_logging()

# Cấu hình logging
logging.basicConfig(level=logging.INFO)

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
        logging.error(f"Crawling error: {e} - URL: {url}")
    return None

def crawl_batch(batch_docs, max_workers=50):
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_doc = {executor.submit(crawl_product_name, doc['url']): doc for doc in batch_docs}

        for future in as_completed(future_to_doc):
            doc = future_to_doc[future]
            try:
                product_name = future.result()
                if product_name:
                    results.append((doc, product_name))
                else:
                    results.append((doc, None))
            except Exception as e:
                logging.error(f"Thread error: {e}")
                results.append((doc, None))

    return results

def claim_batch(col, size=BATCH_SIZE):
    docs = []
    for _ in range(size):
        doc = col.find_one_and_update(
            {"status": "pending"},
            {"$set": {
                "status": "processing",
                "updated_at": datetime.utcnow()
            }},
            sort=[("_id", 1)],
            return_document=ReturnDocument.AFTER
        )
        if not doc:
            break
        print(f"[CLAIMED] _id={doc['_id']} | status={doc.get('status')}")
        docs.append(doc)
    return docs