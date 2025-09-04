from common.dir import BATCH_SIZE
from common.logging_config import setup_logging
from crawler.claim_batch import crawl_product_name, claim_batch
from common.connect import get_mongo_client, MongoConfig, get_collection_name
import time
setup_logging()

def worker(cfg: MongoConfig, col_name="summary"):
    client = get_mongo_client(cfg)
    col = get_collection_name(client, cfg.db_name, col_name)

    while True:
        docs = claim_batch(col, BATCH_SIZE)
        if not docs:
            print("No more jobs. Worker is idle.")
            break

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

            if not pid or not url:
                col.update_one({"_id": doc["_id"]}, {"$set": {"status": "failed"}})
                continue

            product_name = crawl_product_name(url)

            if product_name:
                col.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"status": "done", "product_id": pid,
                              "product_name": product_name, "url": url}}
                )
                print(f"[DONE] {pid} -> {product_name}")
            else:
                col.update_one({"_id": doc["_id"]}, {"$set": {"status": "failed"}})
                print(f"[FAILED] {pid}")

            time.sleep(0.2)
