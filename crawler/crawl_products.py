import logging
import threading
from datetime import datetime, timedelta, timezone
import time
from common.dir import BATCH_SIZE
from common.logging_config import setup_logging
from crawler.claim_batch import crawl_product_name, claim_batch, crawl_batch
from common.connect import get_mongo_client, MongoConfig, get_collection_name
from concurrent.futures import ThreadPoolExecutor, as_completed

setup_logging()
max_workers = 160
def reset_stale_jobs(col, timeout_minutes=30, interval=300):
    """
    Reset các doc đang 'processing' quá lâu về 'pending'.
    - timeout_minutes: sau bao nhiêu phút thì coi là stale
    - interval: bao lâu thì kiểm tra 1 lần (giây)
    """
    while True:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=timeout_minutes)
        result = col.update_many(
            {"status": "processing", "updated_at": {"$lt": cutoff}},
            {"$set": {"status": "pending"}}
        )
        if result.modified_count > 0:
            print(f"[RESET] {result.modified_count} docs reset về pending.")
        time.sleep(interval)
def worker(cfg: MongoConfig, col_name="summary", output_col="products_name"):
    client = get_mongo_client(cfg)
    col = get_collection_name(client, cfg.db_name, col_name)
    crawled_col = get_collection_name(client, cfg.db_name, output_col)
    crawled_col.create_index("product_id", unique=True)
    col.create_index("status")
    threading.Thread(target=reset_stale_jobs, args=(col,), daemon=True).start()
    while True:
        docs = claim_batch(col, BATCH_SIZE)
        if not docs:
            print("No more jobs. Worker is idle.")
            break

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
                col.update_one({"_id": doc["_id"]}, {
                    "$set": {"status": "failed", "updated_at": datetime.utcnow()}
                })
                continue

            if pid and url:
                if crawled_col.find_one({"product_id": pid}):
                    print(f"[SKIP] Product_id {pid} đã tồn tại, bỏ qua.")
                    col.update_one({"_id": doc["_id"]}, {
                        "$set": {"status": "duplicate", "updated_at": datetime.utcnow()}
                    })
                    continue
                url_map[url] = (pid, doc)
            else:
                col.update_one({"_id": doc["_id"]}, {
                    "$set": {"status": "failed", "updated_at": datetime.utcnow()}
                })

        urls = list(url_map.keys())
        results = {}

        with ThreadPoolExecutor(max_workers=160) as executor:
            future_to_url = {executor.submit(crawl_product_name, url): url for url in urls}
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    product_name = future.result()
                    results[url] = product_name
                except Exception as e:
                    msg = f"[FAILED] URL: {url} | Error: {e}"
                    print(msg)
                    results[url] = None

        for url, (pid, doc) in url_map.items():
            try:
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
                                "updated_at": datetime.utcnow(),
                            }
                        },
                        upsert=True
                    )
                    col.update_one({"_id": doc["_id"]}, {
                        "$set": {"status": "done", "updated_at": datetime.utcnow()}
                    })
                    print(f"[DONE] Product_id: {pid} | Name: {product_name}")
                else:
                    col.update_one({"_id": doc["_id"]}, {
                        "$set": {"status": "failed", "updated_at": datetime.utcnow()}
                    })
            except Exception as e:
                col.update_one({"_id": doc["_id"]}, {
                    "$set": {"status": "failed", "updated_at": datetime.utcnow()}
                })
                print(f"[ERROR] {e} | _id={doc['_id']}")