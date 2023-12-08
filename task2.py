import utils

def get_stat_by_salary(collection):
    q = [
        {
            "$group": {
                "_id": "result",
                "max": {"$max": "$salary"},
                "min": {"$min": "$salary"},
                "avg": {"$avg": "$salary"}
            }
        }
    ]

    return list(collection.aggregate(q))

def get_freq_by_job(collection):
    q = [
        {
            "$group": {
                "_id": "$job",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {
                "count": -1
            }
        }
    ]

    return list(collection.aggregate(q))

def get_stat_by_column(collection, group_name, stat_name):
    q = [
        {
            "$group": {
                "_id": f"${group_name}",
                "max": {"$max": f"${stat_name}"},
                "min": {"$min": f"${stat_name}"},
                "avg": {"$avg": f"${stat_name}"}
            }
        }
    ]

    return list(collection.aggregate(q))

def get_max_salary_by_min_age_match(collection):
    q = [
        {
            "$group": {
                "_id": "$age",
                "max_salary": {"$max": "$salary"},
            }
        },
        {
            "$group": {
                "_id": "result",
                "min_age": {"$min": "$_id"},
                "max_salary": {"$max": "$max_salary"}
            }
        }
    ]

    return list(collection.aggregate(q))

def get_min_salary_by_max_age_match(collection):
    q = [
        {
            "$group": {
                "_id": "$age",
                "min_salary": {"$min": "$salary"},
            }
        },
        {
            "$group": {
                "_id": "result",
                "max_age": {"$max": "$_id"},
                "min_salary": {"$min": "$min_salary"}
            }
        }
    ]

    return list(collection.aggregate(q))

def get_sorted_stat_by_condition(collection):
    q = [
        {
            "$match": {
                "salary": {"$gt": 50000}
            }
        },
        {
            "$group": {
                "_id": "$city",
                "min": {"$min": "$age"},
                "max": {"$max": "$age"},
                "avg": {"$avg": "$age"}
            }
        },
        {
            "$sort": {
                "avg": -1
            }
        }
    ]

    return list(collection.aggregate(q))

def get_salary_stat_by_condition(collection):
    q = [
        {
            "$match": {
                "city": {"$in": ["Москва", "Санкт-Петербург", "Барселона", "Гранада"]},
                "job": {"$in": ["IT-специалист", "Инженер", "Врач", "Косметолог"]},
                "$or": [
                    {"age": {"$gt": 17, "$lt": 25}},
                    {"age": {"$gt": 50, "$lt": 65}}
                ]
            }
        }, 
        {
            "$group": {
                "_id": "_result",
                "min": {"$min": "$salary"},
                "max": {"$max": "$salary"},
                "avg": {"$avg": "$salary"},
            }
        }
    ]

    return list(collection.aggregate(q))

# Статистика по зарплатам для позиций с 2000 года
def get_stat_by_job_group(collection):
    q = [
        {
            "$match": {
                "year": {"$gt": 2000}
            }
        },
        {
            "$group": {
                "_id": "$job",
                "min": {"$min": "$salary"},
                "max": {"$max": "$salary"},
                "avg": {"$avg": "$salary"},
            }
        },
        {
            "$sort": {
                "avg": -1
            }
        }
    ]

    return list(collection.aggregate(q))

data = utils.load_json_data("./task2/task_2_item.json")
db = utils.connect_to_mongodb()
#db.person.insert_many(data)

result = get_stat_by_salary(db.person)
utils.write_to_json("./task2/stat_by_salary_result.json", result[0])

result = get_freq_by_job(db.person)
utils.write_to_json("./task2/freq_by_job_result.json", result)

result = get_stat_by_column(db.person, "city", "salary")
utils.write_to_json("./task2/salary_stat_by_city_result.json", result)

result = get_stat_by_column(db.person, "job", "salary")
utils.write_to_json("./task2/salary_stat_by_job_result.json", result)

result = get_stat_by_column(db.person, "city", "age")
utils.write_to_json("./task2/age_stat_by_city_result.json", result)

result = get_stat_by_column(db.person, "job", "age")
utils.write_to_json("./task2/age_stat_by_job_result.json", result)

result = get_max_salary_by_min_age_match(db.person)
utils.write_to_json("./task2/max_salary_by_min_age_result.json", result)

result = get_min_salary_by_max_age_match(db.person)
utils.write_to_json("./task2/min_salary_by_max_age_result.json", result)

result = get_sorted_stat_by_condition(db.person)
utils.write_to_json("./task2/sorted_stat_by_condition_result.json", result)

result = get_salary_stat_by_condition(db.person)
utils.write_to_json("./task2/salary_stat_by_condition_result.json", result[0])

# Произвольный запрос
result = get_stat_by_job_group(db.person)
utils.write_to_json("./task2/stat_by_job_group_result.json", result)
