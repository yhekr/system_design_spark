import json
import random
from datetime import datetime, timedelta
import uuid

DRIVERS = [
    'Быков Фёдор',
    'Волкова Ия',
    'Галкин Максим',
    'Герман Илья',
    'Сизов Михаил',
]

def generate_drivers(n):
    date = "2024-12-18"
    for i in range(1, n + 1):
        driver_id = f"d{i}"

        age = random.randint(12, 80)
        last_name, first_name = random.choice(DRIVERS).split()
        license_id = str(uuid.uuid5(uuid.NAMESPACE_OID, driver_id))

        yield {
            "id": driver_id,
            "info": {
                "first_name": first_name,
                "last_name": last_name,
                "age": age,
                "license_id": license_id,
            },
            "iso_eventtime": f"{date}T00:00:00"
        }

drivers = [driver for driver in generate_drivers(100)]
with open('data/raw/drivers_dump.json', 'w') as logs_file:
    str = json.dumps(drivers)
    print(str, file=logs_file)

with open('data/raw/drivers_dump.json', 'r') as logs_file:
    print(json.load(logs_file))
