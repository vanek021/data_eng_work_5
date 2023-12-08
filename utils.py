import msgpack
import json
import pickle
import csv
import re
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

def load_json_data(path):
    with open(path, "r", encoding="utf-8") as input:
        data = json.load(input)
    return data

def load_pkl_data(path):
    with open(path, "rb") as input:
        items = pickle.load(input)
    return items

def load_json_cru_data(path):
    data = load_json_data(path)
    mongo_data = list()
    
    for data_item in data:
        mongo_item = dict()
        mongo_item['price'] = data_item['marketPrice']
        mongo_item['status'] = "IB" # Note: always IB for Cru
        mongo_item['availability'] = "In stock" # Note: always In Stock for Cry
        mongo_item['packSize'] = data_item['packSize']
        mongo_item['vintage'] = None if data_item['wineName'].endswith("NV") else int(data_item['wineName'][-4:])
        mongo_item['wineName'] = re.sub("\d{4}", "", data_item['wineName'].replace("NV", "").strip()).strip()

        mongo_data.append(mongo_item)
    return mongo_data

def load_csv_chelsea_vintners_data(path):
    mongo_data = []
    with open(path, newline='\n') as file:
        reader = csv.reader(file, delimiter=',')
        is_header = True
        for row in reader:
            if is_header:
                is_header = False
                continue
            price = row[7].replace("Ј ", "").replace(",", ".")
            item = {
                'price': floatTryParse(price),
                'status': row[8],
                'availability': row[9],
                'packSize': row[5],
                'vintage': None if row[0] == "NV" else int(row[0]),
                'wineName':  row[1],
            }
            mongo_data.append(item)
    return mongo_data
    
def connect_to_mongodb(db_name="test-database"):
    client = MongoClient(port=7017)
    db = client[db_name]
    return db

def floatTryParse(value):
    try:
        return float(value)
    except ValueError:
        return None