from common.connect import MongoConfig
from common.dir import DB_NAME, collection_ip_data, dir_out, uri
from importer.read_ips import unique_read_ips
from importer.converter.JsonToJsonLine import convert_json_to_line
from importer.import_ip_location import import_ip_location
from common.logging_config import setup_logging

setup_logging()
if __name__ == '__main__':
    cfg = MongoConfig(
        uri=uri,
        db_name=DB_NAME
    )
    unique_read_ips(cfg,limit=None)
    for i in range(1, 34):
        json_file = f"{dir_out}/ip_batch_{i}.json"
        jsonl_file = convert_json_to_line(json_file)
        import_ip_location(jsonl_file, DB_NAME, collection_ip_data)
