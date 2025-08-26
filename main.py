from config.dir import DB_NAME, collection_ip_data, dir_out
from tasks.read_ips import unique_read_ips
from converter.JsonToJsonLine import convert_json_to_line
from tasks.import_ip_location import import_ip_location
if __name__ == '__main__':
    # unique_read_ips(limit=None)
    for i in range(1, 34):
        json_file = f"{dir_out}/ip_batch_{i}.json"
        jsonl_file = convert_json_to_line(json_file)
        import_ip_location(jsonl_file, DB_NAME, collection_ip_data)