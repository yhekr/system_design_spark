import json
import random
from datetime import datetime, timedelta

def generate_orders(n):
    for i in range(1, n + 1):
        order_id = f"o{i}"
        user_id = random.randint(1, n // 1.3)  # предположительно столько пользователей
        device_id = f"device{i}"

        base_date = datetime(2024, 12, 18)
        random_seconds = random.randint(0, 86399)  # Количество секунд в одном дне

        actual_date = base_date + timedelta(seconds=random_seconds)
        actual_ts = int(actual_date.timestamp())
        actual_iso_eventtime = actual_date.isoformat()

        region_id = random.randint(1, 9)
        
        yield {
            "iso_eventtime": actual_iso_eventtime,
            "event": {
                "event_type": "order_created",
                "event_ts": actual_ts,
                "user_id": user_id,
                "order": {
                    "type": "basic",
                    "id": order_id,
                    "cost": 100.0,
                    "currency": "RUB",
                    "region_id": region_id,
                    "cancel": {
                        "reason": None,
                        "is_driver_cancellation": None
                    }
                },
                "driver_id": None,
                "device_id": device_id
            },
            "source": "some_useless_field"
        }

        # отмена, потому что долго водителя ищем
        if random.random() < 0.1:
            time_increment = random.randint(0, 300)
            actual_date = actual_date + timedelta(seconds=time_increment)
            actual_ts = int(actual_date.timestamp())
            actual_iso_eventtime = actual_date.isoformat()

            driver_cancellation = False
            cancel_reason = random.choice(["Планы поменялись", "машина не двигалась", "тестовое говно, которое пролезло на прод"])

            yield {
                "iso_eventtime": actual_iso_eventtime,
                "event": {
                    "event_type": "order_cancelled",
                    "event_ts": actual_ts,
                    "user_id": user_id,
                    "order": {
                        "type": "basic",
                        "id": order_id,
                        "cost": 100.0,
                        "currency": "RUB",
                        "region_id": region_id,
                        "cancel": {
                            "reason": cancel_reason,
                            "is_driver_cancellation": driver_cancellation
                        }
                    },
                    "driver_id": None,
                    "device_id": device_id
                },
                "source": "some_useless_field"
            }

        # назначаем водителя
        else:
            driver_id = f"d{i}"
            time_increment = random.randint(0, 300)
            actual_date = actual_date + timedelta(seconds=time_increment)
            actual_ts = int(actual_date.timestamp())
            actual_iso_eventtime = actual_date.isoformat()

            yield {
                "iso_eventtime": actual_iso_eventtime,
                "event": {
                    "event_type": "order_assigned",
                    "event_ts": actual_ts,
                    "user_id": user_id,
                    "order": {
                        "type": "basic",
                        "id": order_id,
                        "cost": 100.0,
                        "currency": "RUB",
                        "region_id": region_id,
                        "cancel": {
                            "reason": None,
                            "is_driver_cancellation": None
                        }
                    },
                    "driver_id": driver_id,
                    "device_id": device_id
                },
                "source": "some_useless_field"
            }

            # отмена, потому что водитель не понравился
            if random.random() < 0.13:
                time_increment = random.randint(0, 300)
                actual_date = actual_date + timedelta(seconds=time_increment)
                actual_ts = int(actual_date.timestamp())
                actual_iso_eventtime = actual_date.isoformat()

                if driver_cancellation := random.choice([False, False, False, False, False, False, False, False, False, True]):
                    cancel_reason = False
                else:
                    cancel_reason = random.choice(["Планы поменялись", "машина не двигалась", "тестовое говно, которое пролезло на прод"])

                yield {
                    "iso_eventtime": actual_iso_eventtime,
                    "event": {
                        "event_type": "order_cancelled",
                        "event_ts": actual_ts,
                        "user_id": user_id,
                        "order": {
                            "type": "basic",
                            "id": order_id,
                            "cost": 100.0,
                            "currency": "RUB",
                            "region_id": region_id,
                            "cancel": {
                                "reason": cancel_reason,
                                "is_driver_cancellation": driver_cancellation
                            }
                        },
                        "driver_id": driver_id,
                        "device_id": device_id
                    },
                    "source": "some_useless_field"
                }
            
            # стартуем поездку
            else:
                time_increment = random.randint(60, 600)
                actual_date = actual_date + timedelta(seconds=time_increment)
                actual_ts = int(actual_date.timestamp())
                actual_iso_eventtime = actual_date.isoformat()

                yield {
                    "iso_eventtime": actual_iso_eventtime,
                    "event": {
                        "event_type": "order_started",
                        "event_ts": actual_ts,
                        "user_id": user_id,
                        "order": {
                            "type": "basic",
                            "id": order_id,
                            "cost": 100.0,
                            "currency": "RUB",
                            "region_id": region_id,
                            "cancel": {
                                "reason": None,
                                "is_driver_cancellation": None
                            }
                        },
                        "driver_id": driver_id,
                        "device_id": device_id
                    },
                    "source": "some_useless_field"
                }

                # заказ отменили в процессе
                if random.random() < 0.05:
                    time_increment = random.randint(60, 600)
                    actual_date = actual_date + timedelta(seconds=time_increment)
                    actual_ts = int(actual_date.timestamp())
                    actual_iso_eventtime = actual_date.isoformat()

                    if driver_cancellation := random.choice([False, False, False, False, False, False, False, False, False, True]):
                        cancel_reason = False
                    else:
                        cancel_reason = random.choice(["Планы поменялись", "машина не двигалась", "тестовое говно, которое пролезло на прод"])

                    yield {
                        "iso_eventtime": actual_iso_eventtime,
                        "event": {
                            "event_type": "order_cancelled",
                            "event_ts": actual_ts,
                            "user_id": user_id,
                            "order": {
                                "type": "basic",
                                "id": order_id,
                                "cost": 100.0,
                                "currency": "RUB",
                                "region_id": region_id,
                                "cancel": {
                                    "reason": cancel_reason,
                                    "is_driver_cancellation": driver_cancellation
                                }
                            },
                            "driver_id": driver_id,
                            "device_id": device_id
                        },
                        "source": "some_useless_field"
                    }
                
                # деливеред
                else:
                    time_increment = random.randint(60, 600)
                    actual_date = actual_date + timedelta(seconds=time_increment)
                    actual_ts = int(actual_date.timestamp())
                    actual_iso_eventtime = actual_date.isoformat()

                    yield {
                        "iso_eventtime": actual_iso_eventtime,
                        "event": {
                            "event_type": "order_finished",
                            "event_ts": actual_ts,
                            "user_id": user_id,
                            "order": {
                                "type": "basic",
                                "id": order_id,
                                "cost": 100.0,
                                "currency": "RUB",
                                "region_id": region_id,
                                "cancel": {
                                    "reason": None,
                                    "is_driver_cancellation": None
                                }
                            },
                            "driver_id": driver_id,
                            "device_id": device_id
                        },
                        "source": "some_useless_field"
                    }


orders = [order for order in generate_orders(100)]
with open('system_design_spark/raw/event_log.json', 'w') as logs_file:
    str = json.dumps(orders)
    print(str, file=logs_file)

# with open('system_design_spark/raw/event_log.json', 'r') as logs_file:
#     print(json.load(logs_file))


