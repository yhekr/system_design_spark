import json
import random
from datetime import datetime, timedelta

USERS = [
    'Быков Фёдор',
    'Волкова Ия',
    'Галкин Максим',
    'Герман Илья',
    'Сизов Михаил',
]

def generate_users(n):
    date = "2024-12-18"
    for user_id in range(1, n + 1):
        age = random.randint(12, 80)
        last_name, first_name = random.choice(USERS).split()

        yield {
            "id": user_id,
            "info": {
                "first_name": first_name,
                "last_name": last_name,
                "age": age
            },
            "iso_eventtime": f"{date}T00:00:00"
        }

users = [user for user in generate_users(100)]
with open('system_design_spark/raw/users_dump.json', 'w') as logs_file:
    str = json.dumps(users)
    print(str, file=logs_file)

# with open('system_design_spark/raw/users_dump.json', 'r') as logs_file:
#     print(json.load(logs_file))
