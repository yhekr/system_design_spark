import json
import random
from datetime import datetime, timedelta

USERS = [
    'Соколов Евгений',
    'Трушин Дима',
    'Аржанцев Иван',
    'Алексей Сальников'
]

def generate_users(n):
    ts = datetime.now()
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
            "iso_eventtime": ts.isoformat()
        }


def users_dump(**kwargs):
    users = [user for user in generate_users(1000)]
    with open('data/raw/users_dump.json', 'w') as logs_file:
        str = json.dumps(users)
        print(str, file=logs_file)

# with open('data/raw/users_dump.json', 'r') as logs_file:
#     print(json.load(logs_file))
