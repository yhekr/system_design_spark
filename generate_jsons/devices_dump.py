import json
import random
from datetime import datetime, timedelta

PLATFORMS = [
    'iOS',
    'Android',
    'HarmonyOS',
]

VERSIONS = {
    'iOS': [17, 18],
    'Android': [12, 15],
    'HarmonyOS': [4, 5]
}

def generate_devices(n):
    ts = datetime(2024, 12, 18, 0, 0, 0).isoformat()
    for device_id in range(1, n + 1):
        platform = random.choice(PLATFORMS)
        version = random.randint(VERSIONS[platform][0], VERSIONS[platform][1])

        yield {
            "iso_timestamp": ts,
            "info": {
                "platform": platform,
                "version": version,
                "device_id": device_id
            },
        }

devices = [device for device in generate_devices(100)]
with open('data/raw/devices_dump.json', 'w') as logs_file:
    str = json.dumps(devices)
    print(str, file=logs_file)

# with open('data/raw/users_dump.json', 'r') as logs_file:
#     print(json.load(logs_file))
