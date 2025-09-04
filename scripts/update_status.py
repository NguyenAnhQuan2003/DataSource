from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["glamira"]
collection = db["summary"]

batch_size = 500_000
count = collection.count_documents({})
print(f"Tổng số document: {count}")

last_id = None
updated = 0

while True:
    # Lấy batch _id
    query = {"_id": {"$gt": last_id}} if last_id else {}
    docs = list(collection.find(query, {"_id": 1}).sort("_id", 1).limit(batch_size))

    if not docs:
        break

    ids = [d["_id"] for d in docs]
    last_id = ids[-1]

    # Update batch
    result = collection.update_many(
        {"_id": {"$in": ids}},
        {"$set": {"status": "pending"}}
    )

    updated += result.modified_count
    print(f"Đã update: {updated}/{count}")

print("Hoàn thành!")
