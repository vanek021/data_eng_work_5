import utils

def write_sorted_by_salary(collection):
    utils.write_to_json("./task1/sort_desc_result.json", list(collection.find(limit=10).sort({'salary': -1})))

def write_age_filtered_salary_sorted(collection):
    utils.write_to_json("./task1/filter_sort_desc_result.json", list(collection.find({"age": {"$lt": 30}}, limit=15).sort({"salary": -1})))

def complex_filter_by_city_and_job(collection):
    utils.write_to_json("./task1/complex_filter_result.json", 
        collection.find({"city": "Санкт-Петербург", "job": {"$in": ["Учитель", "Программист", "Бухгалтер"]}}, limit=10).sort({'age': 1}))
    
def count_obj(collection):
    utils.write_to_json("./task1/count_obj_result.json",                  
        collection.count_documents({
            "age": {"$gt": 25, "$lt": 40},
            "year": {"$gt": 2019, "$lte": 2022},
            "$or": [
                {"salary": {"$gt": 50000, "$lte": 75000}},
                {"salary": {"$gt": 125000, "$lt": 150000}}
            ]
        })
    )

data = utils.load_msgpack_data("./task1/task_1_item.msgpack")
db = utils.connect_to_mongodb()
#db.person.insert_many(data)

write_sorted_by_salary(db.person)
write_age_filtered_salary_sorted(db.person)
complex_filter_by_city_and_job(db.person)
count_obj(db.person)