from config.connect import connect_mongodb
from tasks.read_ips import unique_read_ips
if __name__ == '__main__':
    db = connect_mongodb()