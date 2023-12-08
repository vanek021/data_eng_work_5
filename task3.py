import utils

def delete_by_salary(collection):
    result = collection.delete_many({
        "$or": [
            {"salary": {"$lt": 25000}},
            {"salary": {"$gt": 175000}}
        ]
    })

    print(result)

def update_age(collection):
    result = collection.update_many({}, {
        "$inc": {
            "age": 1
        }
    })

    print(result)

def increase_salary_by_job(collection):
    filter = {
        "job": {"$in": ["Учитель", "Косметолог", "Медсестра", "IT-специалист"]}
    }
    
    update = {
        "$mul": {
            "salary": 1.05
        }
    }

    result = collection.update_many(filter, update)
    print(result)

def increase_salary_by_city(collection):
    filter = {
        "city": {"$in": ["Москва", "Санкт-Петербург", "Барселона", "Гранада"]}
    }

    update = {
        "$mul": {
            "salary": 1.07
        }
    }

    result = collection.update_many(filter, update)
    print(result)

def increase_salary_by_many_conditions(collection):
    filter = {
        "city": {"$in": ["Москва", "Санкт-Петербург", "Барселона", "Гранада"]},
        "job": {"$in": ["Учитель", "Косметолог", "Медсестра", "IT-специалист"]},
        "age": {"$gt": 20, "$lt": 50},
    }

    update = {
        "$mul": {
            "salary": 1.1
        }
    }

    result = collection.update_many(filter, update)
    print(result)

# Удалить данные ранее 2000 года, с возрастом меньше 18 или больше 55
def delete_by_many_conditions(collection):
    result = collection.delete_many({
        "year": {"$lt": 2000},
        "$or": [
            {"age": {"$lt": 18}},
            {"age": {"$gt": 55}}
        ]
    })

    print(result)

data = utils.load_pkl_data("./task3/task_3_item.pkl")
db = utils.connect_to_mongodb()
#db.person.insert_many(data)

delete_by_salary(db.person)
update_age(db.person)
increase_salary_by_job(db.person)
increase_salary_by_city(db.person)
increase_salary_by_many_conditions(db.person)
delete_by_many_conditions(db.person)