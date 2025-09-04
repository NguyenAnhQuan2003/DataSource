#logging
#address_log = "/home/decquannguyen/DataSource/logs/app.log"
address_log = "/Users/nguyenanhquan/Desktop/Project5/logs/app.log"
#DB connects
#user_name = "nguyenanhquan"
user_name = "crawler4"
pwd = "password4"
host = "35.223.225.198"
port = "27017"
DB_NAME = "glamira"
uri = f"mongodb://{user_name}:{pwd}@{host}:{port}"
#uri = "mongodb://localhost:27017"
collection_ = "summary"
collection_ip_data = "ip_location"
#output file
# dir_out = "/home/decquannguyen/DataSource/output/ip_json_files"
# dir_export = "/home/decquannguyen/DataSource/output/csv_files"
dir_out = "/Users/nguyenanhquan/Desktop/Project5/output/ip_json_files"
dir_export = "/Users/nguyenanhquan/Desktop/Project5/output/csv_files"
BATCH_SIZE = 500
