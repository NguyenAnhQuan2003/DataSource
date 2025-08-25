import logging
import IP2Location
import redis
from config.connect import connect_mongodb
from config.dir import collection_, collection_ip_data
from config.logging_config import setup_logging
setup_logging()

def unique_read_ips(limit=None, batch_size=10000):
    db = connect_mongodb()
    src_col = db[collection_]
    dst_col = db[collection_ip_data]
    ip2loc = IP2Location.IP2Location("IP-COUNTRY-REGION-CITY.BIN")
    r = redis.Redis(host="localhost", port=6379, db=0)
    query = src_col.find({}, {"ip": 1})
    if limit:
        query = query.limit(limit)

    buffer = []
    count = 0
    for i, d in enumerate(query, start=1):
        ip = d.get("ip")
        if not ip:
            continue

        try:
            # check redis cache trước
            cached = r.get(ip)
            if cached:
                rec = eval(cached.decode())  # lấy từ redis (string -> dict)
            else:
                lookup = ip2loc.get_all(ip)
                rec = {
                    "country_short": lookup.country_short,
                    "country_long": lookup.country_long,
                    "region": lookup.region,
                    "city": lookup.city,
                    "latitude": lookup.latitude,
                    "longitude": lookup.longitude,
                }
                r.set(ip, str(rec))  # cache vào Redis

            new_doc = {"ip": ip, **rec}
            buffer.append(new_doc)

        except Exception as e:
            logging.error(f"[{i}] Lỗi enrich IP {ip}: {e}")

        if len(buffer) >= batch_size:
            dst_col.insert_many(buffer)
            count += len(buffer)
            logging.info(f"Đã insert {count} documents...")
            buffer.clear()

    if buffer:
        dst_col.insert_many(buffer)
        count += len(buffer)
        logging.info(f"Hoàn tất insert {count} documents.")