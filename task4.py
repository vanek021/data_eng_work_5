import utils

cru_data = utils.load_json_cru_data("./task4/wine_data_cru.json")
chelsea_vintners_data = utils.load_csv_chelsea_vintners_data("./task4/wine_data_chelsea_vintners.csv")

db = utils.connect_to_mongodb()

# Вставка данных

#db.wine.insert_many(cru_data)
#db.wine.insert_many(chelsea_vintners_data)

#region Выборка

# Поиск вин без винтажа (года выпуска), т.е NV (Non-Vintage)
def get_wines_without_vintage(collection):
    return list(collection.find({"vintage": None}))

# Поиск вин без цен
def get_wines_without_price(collection):
    return list(collection.find({"price": None}))

# Поиск доступных вин
def get_wines_in_stock(collection):
    return list(collection.find({"availability": "In stock"}))

# Поиск вин в определенном интервале винжата
def get_wines_with_vintage_from_to(collection, vintage_from, vintage_to):
    return list(collection.find({"vintage": {"$gt": vintage_from, "$lt": vintage_to}}))

# Поиск вин по размеру упаковки
def get_wines_by_pack_size(collection, pack_size):
    return list(collection.find({"packSize": pack_size}))

result = get_wines_without_vintage(db.wine)
utils.write_to_json("./task4/find/wines_without_vintage_result.json", result)

result = get_wines_without_price(db.wine)
utils.write_to_json("./task4/find/wines_without_price_result.json", result)

result = get_wines_in_stock(db.wine)
utils.write_to_json("./task4/find/wines_in_stock_result.json", result)

result = get_wines_with_vintage_from_to(db.wine, 2010, 2020)
utils.write_to_json("./task4/find/wines_with_vintage_from_to_result.json", result)

result = get_wines_by_pack_size(db.wine, "6x75cl")
utils.write_to_json("./task4/find/wines_by_pack_size_result.json", result)

#endregion

#region Выборка с агрегацией

# Группировка по статусу доступности
def get_groupped_by_availability(collection):
    q = [
        {
            "$group": {
                "_id": "$availability",
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

# Группировка по размеру коробки
def get_groupped_by_pack_size(collection):
    q = [
        {
            "$group": {
                "_id": "$packSize",
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

# Статистика по винам
def get_stat_by_wines(collection):
    q = [
        {
            "$group": {
                "_id": "$wineName",
                "max": {"$max": "$price"},
                "min": {"$min": "$price"},
                "avg": {"$avg": "$price"},
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

# Статистика по винам с отфильтрованной ценой
def get_sorted_stat_by_price(collection):
    q = [
        {
            "$match": {
                "price": {"$gt": 30} # Note: здравый смысл
            }
        },
        {
            "$group": {
                "_id": "$wineName",
                "min": {"$min": "$price"},
                "max": {"$max": "$price"},
                "avg": {"$avg": "$price"}
            }
        },
        {
            "$sort": {
                "avg": -1
            }
        }
    ]

    return list(collection.aggregate(q))

def get_min_price_by_min_vintage(collection):
    q = [
        {
            "$match": {
                "vintage": {"$ne": None},
                "price": {"$ne": None}
            }
        },
        {
            "$group": {
                "_id": "$vintage",
                "min_price": {"$min": "$price"},
            }
        },
        {
            "$group": {
                "_id": "result",
                "min_price": {"$min": "$min_price"},
                "min_vintage": {"$min": "$_id"}
            }
        }
    ]

    return list(collection.aggregate(q))

result = get_groupped_by_availability(db.wine)
utils.write_to_json("./task4/aggregate/groupped_by_availability_result.json", result)

result = get_groupped_by_pack_size(db.wine)
utils.write_to_json("./task4/aggregate/groupped_by_pack_size_result.json", result)

result = get_stat_by_wines(db.wine)
utils.write_to_json("./task4/aggregate/stat_by_wines_result.json", result)

result = get_sorted_stat_by_price(db.wine)
utils.write_to_json("./task4/aggregate/sorted_stat_by_price_result.json", result)

result = get_min_price_by_min_vintage(db.wine)
utils.write_to_json("./task4/aggregate/min_price_by_min_vintage_result.json", result)

#endregion

#region Обновление/удаление данных

# Удаление вин с ценой меньше 20
def delete_by_price(collection):
    result = collection.delete_many({"price": {"$lt": 20}})
    print(result)

# Увеличение стоимости вин на 5%, если вино стоит меньше 30 GBP
def increase_wine_price(collection):
    filter = {
        "price": {"$lt": 30}
    }
    
    update = {
        "$mul": {
            "price": 1.05
        }
    }

    result = collection.update_many(filter, update)
    print(result)

# Увеличение стоимости на 10% для вин с датой производства раньше 1950, со статусом In Bond, с доступностью In Stock
def increase_wine_price_by_many_conditions(collection):
    filter = {
        "vintage": {"$lt": 1950},
        "status": "IB",
        "availability": "In stock",
    }

    update = {
        "$mul": {
            "salary": 1.1
        }
    }

    result = collection.update_many(filter, update)
    print(result)

# Удалить записи по определенному размеру упаковки
def delete_by_pack_size(collection, pack_size):
    result = collection.delete_many({"packSize": pack_size})
    print(result)

# Увеличить стоимость вина, если цена 750, доступность - In Stock, статус IB или DP, и год больше 2000
def decrease_wine_price(collection):
    filter = {
        "vintage": {"$gte": 2000},
        "status": {"$in": ["IB", "DP"]},
        "availability": "In stock",
        "price": {"$gte": 750}
    }

    update = {
        "$mul": {
            "salary": 0.9
        }
    }

    result = collection.update_many(filter, update)
    print(result)

delete_by_price(db.wine)
delete_by_pack_size(db.wine, "70cl")
increase_wine_price(db.wine)
increase_wine_price_by_many_conditions(db.wine)
decrease_wine_price(db.wine)

#endregion