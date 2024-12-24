import random
import numpy as np
from datetime import datetime
from scipy.stats import rv_continuous
import math
import uuid
import json

EVENT_TYPE = ['CREATED', 'ASSIGNED', 'STARTED', 'COMPLETED', 'CANCELED']
# Possible system behaviour:
# created -> assigned -> started -> completed  75%
# created -> canceled                          13%
# created -> assigned -> canceled              10% 
# created -> assigned -> started -> canceled   2%
COUNTRIES = ['Russia', 'Belarus', 'Kazakhstan']
CITIES_LIST = ['Moscow', 'Saint Petersburg', 'Nizhniy Novgorod', 'Minsk', 'Grodno', 'Astana', 'Almaty']
p_countries = [0.8, 0.1, 0.1]
CITIES = {
    'Russia': (['Moscow', 'Saint Petersburg', 'Nizhniy Novgorod'], [0.5, 0.3, 0.2]),
    'Belarus': (['Minsk', 'Grodno'], [0.7, 0.3]),
    'Kazakhstan': (['Astana', 'Almaty'], [0.6, 0.4])
}

CAR_MODELS = ['Lada', 'Kia', 'Hyundai']
CAR_SUBMODELS = {
    'Lada': ['Granta', 'Vesta'],
    'Hyundai': ['Solaris', 'Accent'],
    'Kia': ['Ceed', 'Soul']
}

CURR = {
    'Russia': 'RUB',
    'Belarus': 'BYN',
    'Kazakhstan': 'KZT'
}

CITIES_COST = {'Moscow': [100, 1000], 'Saint Petersburg': [100, 700], 'Nizhniy Novgorod': [50, 500], 'Minsk': [2, 20], 'Grodno': [1, 15], 'Astana': [500, 3000], 'Almaty': [500, 3000]}

FLAGS = ([0, 1], [0.99, 0.01])

DRIVER_NAME = ['Djonidep', 'Babidjon', 'Chumaboi', 'Nasirullo', 'Nikolay', 'Oleg', 'Vladimir']

OS_NAME = ['iOS', 'Android']
OS_VERSION = {'iOS': [17, 18], 'Android': [14, 15]}

cars = []
def gen_car_number():
    while True:
        l = ['A', 'B', 'E', 'K', 'M', 'H', 'O', 'P', 'C', 'T', 'Y', 'X']
        n = []
        for i in range (6):
            if i >= 1 and i <= 3:
                n.append(random.randint(0, 9))
            else:
                n.append(l[random.randint(0, 11)])
        s = ''.join(str(i) for i in n)
        if cars.count(s) == 0:
            cars.append(s)
            return s
    

def show(random_numbers):
    import matplotlib.pyplot as plt
    import seaborn as sns
    sns.histplot(random_numbers, kde=True, bins=20, color="blue", edgecolor="black", alpha=0.7)
    plt.title("Распределение данных")
    plt.xlabel("Значения")
    plt.ylabel("Плотность/Частота")
    plt.show()

class OrderDistribution(rv_continuous):
    def _pdf(self, x):
        return 0.3 * math.sin(math.pi * x * 2 - 1) + 1

def toTs(ts_raw, date):
    ts = date
    # ts = date.timestamp()
    ts += int(86400 * ts_raw)
    return ts
    # return datetime.fromtimestamp(ts)

def generateLogs(log_cnt, date, drivers_count, users_count):
    date = date.timestamp()

    # timestamps
    order_dist = OrderDistribution(a=0, b=1)
    timestamps_raw = order_dist.rvs(size=log_cnt)
    timestamps_raw = sorted(timestamps_raw)
    timestamps = [toTs(ts_raw, date) for ts_raw in timestamps_raw]

    #drivers
    drivers = []
    driver_names = []
    cars = []
    car_models = []
    car_submodels = []
    for i in range(drivers_count):
        cars.append(gen_car_number())
        drivers.append(uuid.uuid4())
        driver_names.append(DRIVER_NAME[random.randint(0, 6)])
        car_models.append(CAR_MODELS[random.randint(0, 2)])
        car_submodels.append(CAR_SUBMODELS[car_models[i]][random.randint(0, 1)])

    #users
    puids = []
    device_ids = []
    os_name = []
    os_vesrion = []
    for i in range(users_count):
        puids.append(uuid.uuid4())
        device_ids.append(uuid.uuid4())
        os_name.append(OS_NAME[random.randint(0, 1)])
        os_vesrion.append(OS_VERSION[os_name[i]][random.randint(0, 1)])


    table = [[] for i in range(log_cnt)]
    used = [0 for i in range(log_cnt)]

    used_users = [0 for i in range(users_count)]
    used_drivers = [0 for i in range(drivers_count)]

    for i in range(log_cnt):
        if not used[i]:
            used[i] = 1
            country = np.random.choice(COUNTRIES, size=1, p=p_countries)[0]
            city = np.random.choice(CITIES[country][0], size=1, p=CITIES[country][1])[0]
            country_id = COUNTRIES.index(country)
            city_id = CITIES_LIST.index(city)
            flgs = np.random.choice(FLAGS[0], size=5, p=FLAGS[1]) 
            ab_flags = f'{flgs[0]}{flgs[1]}{flgs[2]}{flgs[3]}{flgs[4]}'
            currecy = CURR[country]
            cost_lcl = random.randint(CITIES_COST[city][0], CITIES_COST[city][1])
            user = random.randint(0, users_count - 1)
            driver = random.randint(0, drivers_count - 1)

            used_users[user] = 1
            used_drivers[driver] = 1

            row = [EVENT_TYPE[0], timestamps[i], puids[user], device_ids[user], drivers[driver], driver_names[driver], cars[driver], cost_lcl, currecy, city_id, city, country_id, country, ab_flags]
            table[i] = row.copy()

            t = random.randint(5, 120)
            next_t = timestamps[i] + t
            j = i
            while j < log_cnt and (timestamps[j] < next_t or used[j] == 1):
                j += 1

            if j != log_cnt:
                used[j] = 1
                if random.randint(1, 100) <= 13: #cancel
                    row[0] = EVENT_TYPE[4]
                    row[1] = timestamps[j]
                    table[j] = row
                else:
                    row[0] = EVENT_TYPE[1]
                    row[1] = timestamps[j]
                    table[j] = row

        else:
            if table[i][0] == EVENT_TYPE[1]:
                if random.randint(1, 87) <= 10: #cancel
                    row = table[i].copy()
                    row[0] = EVENT_TYPE[4]

                    t = random.randint(1, 60)
                    next_t = timestamps[i] + t
                    j = i
                    while j < log_cnt and (timestamps[j] < next_t or used[j] == 1):
                        j = j + 1

                    if j != log_cnt:
                        used[j] = 1
                        row[1] = timestamps[j]
                        table[j] = row
                else: 
                    row = table[i].copy()
                    row[0] = EVENT_TYPE[2]

                    t = random.randint(60, 600)
                    next_t = timestamps[i] + t
                    j = i
                    while j < log_cnt and (timestamps[j] < next_t or used[j] == 1):
                        j = j + 1

                    if j != log_cnt:
                        used[j] = 1
                        row[1] = timestamps[j]
                        table[j] = row
            elif table[i][0] == EVENT_TYPE[2]:
                if random.randint(1, 77) <= 2: #cancel
                    row = table[i].copy()
                    row[0] = EVENT_TYPE[4]

                    t = random.randint(30, 120)
                    next_t = timestamps[i] + t
                    j = i
                    while j < log_cnt and (timestamps[j] < next_t or used[j] == 1):
                        j = j + 1

                    if j != log_cnt:
                        used[j] = 1
                        row[1] = timestamps[j]
                        table[j] = row
                else: 
                    row = table[i].copy()
                    row[0] = EVENT_TYPE[3]

                    t = random.randint(300, 3600)
                    next_t = timestamps[i] + t
                    j = i
                    while j < log_cnt and (timestamps[j] < next_t or used[j] == 1):
                        j = j + 1

                    if j != log_cnt:
                        used[j] = 1
                        row[1] = timestamps[j]
                        table[j] = row

    logs_json = []
    for i in range(log_cnt):
        logs_json.append({
            'event_type': table[i][0],
            'msk_event_dttm': int(table[i][1]),
            'puid': str(table[i][2]),
            'device_id': str(table[i][3]),
            'driver_id': str(table[i][4]),
            # 'driver_name': table[i][5],
            'car_registration_number': table[i][6],
            'cost_lcl': table[i][7],
            'currency': table[i][8],
            'city_id': table[i][9],
            'city_name': table[i][10],
            'country_id': table[i][11],
            'country_name': table[i][12],
            'ab_flags': table[i][13]
        })

    # from pprint import pprint
    # print(logs_json)
    with open("raw_logs.json", "w", encoding="utf-8") as file:
        json.dump(logs_json, file, ensure_ascii=False, indent=4)

    users_json = []
    devices_json = []
    for i in range(users_count):
        if used_users[i]:
            users_json.append({
                'puid': str(puids[i])
            })
            devices_json.append({
                'device_id': str(device_ids[i]),
                'paltform_name': os_name[i],
                'platform_version': os_vesrion[i]
            })

    drivers_json = []
    for i in range(drivers_count):
        if used_drivers[i]:
            drivers_json.append({
                'driver_id': str(drivers[i]),
                'driver_name': driver_names[i],
                'car_model': car_models[i],
                'car_submodel': car_submodels[i],
                'car_registration_number': cars[i]
            })

    with open("raw_users.json", "w", encoding="utf-8") as file:
        json.dump(users_json, file, ensure_ascii=False, indent=4)
    
    with open("raw_devices.json", "w", encoding="utf-8") as file:
        json.dump(devices_json, file, ensure_ascii=False, indent=4)

    with open("raw_drivers.json", "w", encoding="utf-8") as file:
        json.dump(drivers_json, file, ensure_ascii=False, indent=4)

    currencies_json = [
        {'currency': 'RUB', 'rate': 1.0},
        {'currency': 'BYN', 'rate': 30.72},
        {'currency': 'KZT', 'rate': 0.19}
    ]

    with open("raw_currencies.json", "w", encoding="utf-8") as file:
        json.dump(currencies_json, file, ensure_ascii=False, indent=4)

if __name__ == '__main__':
    generateLogs(50000, datetime(2024, 12, 23, 0, 0, 0), 3000, 30000)
    # generateLogs(10, datetime(2024, 12, 23, 0, 0, 0), 3, 5)
    