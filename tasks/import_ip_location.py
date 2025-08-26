import subprocess
from config.dir import uri
def import_ip_location(jsonl_file, db_name, collection_name):
    cmd = [
        "mongoimport",
        "--uri", uri,
        "--db", db_name,
        "--collection", collection_name,
        "--file", str(jsonl_file),
        "--jsonArray"
    ]
    subprocess.run(cmd, check=True)
    print(f"Import xong {jsonl_file} vao {db_name}.{collection_name}\n")
