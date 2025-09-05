import logging

from common.dir import BATCH_SIZE
from common.logging_config import setup_logging
from crawler.claim_batch import crawl_product_name, claim_batch, crawl_batch
from common.connect import get_mongo_client, MongoConfig, get_collection_name
from concurrent.futures import ThreadPoolExecutor, as_completed

setup_logging()
max_workers = 50
def worker(cfg: MongoConfig, col_name="summary", output_col="products_name"):
    client = get_mongo_client(cfg)
    col = get_collection_name(client, cfg.db_name, col_name)
    crawled_col = get_collection_name(client, cfg.db_name, output_col)
    while True:
        docs = claim_batch(col, BATCH_SIZE)
        if not docs:
            print("No more jobs. Worker is idle.")
            break

        # Chuẩn bị URL map
        url_map = {}
        for doc in docs:
            if doc["collection"] in [
                "view_product_detail", "select_product_option",
                "select_product_option_quality", "add_to_cart_action",
                "product_detail_recommendation_visible",
                "product_detail_recommendation_noticed"
            ]:
                pid = doc.get("product_id") or doc.get("viewing_product_id")
                url = doc.get("current_url")
            elif doc["collection"] == "product_view_all_recommend_clicked":
                pid = doc.get("viewing_product_id")
                url = doc.get("referrer_url")
            else:
                col.update_one({"_id": doc["_id"]}, {"$set": {"status": "failed"}})
                continue

            if pid and url:
                url_map[url] = (pid, doc)
            else:
                col.update_one({"_id": doc["_id"]}, {"$set": {"status": "failed"}})

        urls = list(url_map.keys())
        results = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(crawl_product_name, url): url for url in urls}

            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    product_name = future.result()  # crawl_product_name trả về string
                    results[url] = product_name
                except Exception as e:
                    logging.error(f"Crawl failed for {url}: {e}")
                    results[url] = None
                    results[url] = None

        for url, (pid, doc) in url_map.items():
            product_name = results.get(url)
            if product_name:
                crawled_col.update_one(
                    {"product_id": pid},
                    {
                        "$set": {
                            "product_id": pid,
                            "product_name": product_name,
                            "url": url,
                            "source_doc_id": doc["_id"],
                        }
                    },
                    upsert=True
                )
                col.update_one({"_id": doc["_id"]}, {"$set": {"status": "done"}})
                logging.info(f"[DONE] Product_id: {pid} | Name: {product_name}")
            else:
                col.update_one({"_id": doc["_id"]}, {"$set": {"status": "failed"}})