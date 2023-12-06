import msgpack
import json
from pymongo import MongoClient
from bson import json_util

VAR = 29

def write_to_json(path: str, data: str):
    with open(path, 'w', encoding="utf-8") as f:
        f.write(json_util.dumps(data, ensure_ascii=False))

def load_msgpack_data(path):
    with open(path, "rb") as input:
        data = msgpack.load(input)
    return data

def connect_to_mongodb(db_name="test-database"):
    client = MongoClient(port=7017)
    db = client[db_name]
    return db