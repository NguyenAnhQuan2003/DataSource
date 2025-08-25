import logging
import IP2Location
import json
import redis
from config.connect import connect_mongodb
from config.dir import collection_, collection_ip_data
from config.logging_config import setup_logging
setup_logging()
BATCH_SIZE = 100_000
OUTPUT_DIR = "./output/ip_json_files/"
def unique_read_ips(limit=None):
    db = connect_mongodb()
    src_col = db[collection_]
    # dst_col = db[collection_ip_data]
    ip2loc = IP2Location.IP2Location("IP-COUNTRY-REGION-CITY.BIN")
    # r = redis.Redis(host="localhost", port=6379, db=0)
    # query = src_col.find({}, {"ip": 1})
    # if limit:
    #     query = query.limit(limit)
    #
    # buffer = []
    # count = 0
    pipeline = [
        {"$group": {"_id": "$ip"}}
    ]
    if limit:
        pipeline.append({"$limit": limit})

    cursor = src_col.aggregate(pipeline, allowDiskUse=True)
    buffer = []
    file_count = 1
    # for i, d in enumerate(query, start=1):
    #     ip = d.get("ip")
    #     if not ip:
    #         continue
    #
    #     try:
    #         cached = r.get(ip)
    #         if cached:
    #             rec = eval(cached.decode())
    #         else:
    #             lookup = ip2loc.get_all(ip)
    #             rec = {
    #                 "country_short": lookup.country_short,
    #                 "country_long": lookup.country_long,
    #                 "region": lookup.region,
    #                 "city": lookup.city,
    #             }
    #             r.set(ip, str(rec))
    #
    #         new_doc = {"ip": ip, **rec}
    #         buffer.append(new_doc)
    #
    #     except Exception as e:
    #         logging.error(f"[{i}] Lỗi enrich IP {ip}: {e}")
    #
    #     if len(buffer) >= batch_size:
    #         dst_col.insert_many(buffer)
    #         count += len(buffer)
    #         logging.info(f"Đã insert {count} documents...")
    #         buffer.clear()
    #
    # if buffer:
    #     dst_col.insert_many(buffer)
    #     count += len(buffer)
    #     logging.info(f"Hoàn tất insert {count} documents.")
    for i, doc in enumerate(cursor, start=1):
        ip = doc["_id"]
        if not ip:
            continue

        try:
            lookup = ip2loc.get_all(ip)
            rec = {
                "ip": ip,
                "country_short": lookup.country_short,
                "country_long": lookup.country_long,
                "region": lookup.region,
                "city": lookup.city,
            }
            buffer.append(rec)
        except Exception as e:
            logging.error(f"[{i}] Lỗi enrich IP {ip}: {e}")

        if len(buffer) >= BATCH_SIZE:
            file_path = f"{OUTPUT_DIR}ip_batch_{file_count}.json"
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(buffer, f, ensure_ascii=False, indent=2)
            logging.info(f"Đã lưu batch {file_count} với {len(buffer)} IP")
            buffer.clear()
            file_count += 1

    if buffer:
        file_path = f"{OUTPUT_DIR}ip_batch_{file_count}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(buffer, f, ensure_ascii=False, indent=2)
        logging.info(f"Hoàn tất lưu batch cuối với {len(buffer)} IP")