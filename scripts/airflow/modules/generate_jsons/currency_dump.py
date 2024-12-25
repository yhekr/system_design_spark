import xmltodict
import requests
import datetime
import json
import schedule
import time

url = 'http://www.cbr.ru/scripts/XML_daily.asp'

def getDailyUrl(year, month, day):
    return f'{url}?date_req={day}/{month}/{year}'

CURRENCIES = ['RUB', 'BYN', 'KZT']

def get_currencies():
    date = datetime.datetime.today()
    cbr_responce = requests.get(getDailyUrl(date.year, date.month, date.day))
    data = xmltodict.parse(cbr_responce.content)
    currencies = data['ValCurs']['Valute']
    
    for currency in currencies:
        if currency['CharCode'] in CURRENCIES:
            value_str = str(currency['VunitRate'])
            value = float(value_str.replace(',', '.'))

            yield {
                "iso_timestamp": datetime.datetime(date.year, date.month, date.day, 0, 0, 0).isoformat(),
                "info": {
                    "currency": currency['CharCode'],
                    "value": value
                }
            }

    yield {
        "iso_timestamp": datetime.datetime(date.year, date.month, date.day, 0, 0, 0).isoformat(),
        "info": {
            "currency": 'RUB',
            "value": 1.0
        }
    }

def updateCurrencies(): # выгружает курсы на сегодняшний день
    currencies = [currency for currency in get_currencies()]
    with open('data/raw/currencies_dump.json', 'w') as logs_file:
        str = json.dumps(currencies)
        print(str, file=logs_file)

schedule.every(1).day.do(updateCurrencies)

def updatingCurrenciesProcess(): # раз в день запускает updateCurrencies()
    while True:
        schedule.run_pending()
        time.sleep(1)