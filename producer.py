from __future__ import annotations
from kafka import KafkaProducer
from typing import Dict
from json import dumps
import requests
import time
import sys


def coincap_parsing(obj: Dict[str, str | int | float]
                    ) -> Dict[str, str | int | float]:
    try:
        obj['rank'] = int(obj['rank'])
    except TypeError as e:
        pass
    try:
        obj["percentTotalVolume"] = float(obj["percentTotalVolume"])
    except TypeError as e:
        pass
    try:
        obj["volumeUsd"] = float(obj["volumeUsd"])
    except TypeError as e:
        pass
    try:
        obj["tradingPairs"] = int(obj["tradingPairs"])
    except TypeError as e:
        pass
    return obj


def fetch_write_topic(url: str, producer: KafkaProducer, topic: str) -> bool:
    try:
        response = requests.get(url)
        if response.status_code == 200:
            for obj in response.json()['data']:
                if topic == 'coincap_exchanges':
                    obj = coincap_parsing(obj)
                producer.send(topic, obj)
            return True
        else:
            return False
    except requests.exceptions.RequestException as e:
        print('Connection error!')
        return False


def print_in_line(text: str) -> None:
    sys.stdout.write('\r' + text)
    sys.stdout.flush()


def main(base_url: str, exchange_topic: str, assets_topic: str,
         kafka_server: str) -> None:
    kafka_producer = KafkaProducer(
        bootstrap_servers=kafka_server,
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )
    exchange_url = base_url + '/exchanges'
    assets_url = base_url + '/assets'
    api_calls = 0
    successful_exchanges = 0
    successful_assets = 0
    while True:
        start = time.time()
        if fetch_write_topic(exchange_url, kafka_producer, exchange_topic):
            successful_exchanges += 1
        time.sleep(0.1)
        if fetch_write_topic(assets_url, kafka_producer, assets_topic):
            successful_assets += 1
        api_calls += 1

        print_in_line(f'Total API Calls: {api_calls}, '
                      f'Successful Exchange API Calls: {successful_exchanges}, '
                      f'Successful Assets API Calls: {successful_assets}, '
                      f'Time on 2 API Calls: {time.time() - start:.2f}')
        time.sleep(0.5)


if __name__ == '__main__':
    BASE_URL = "https://api.coincap.io/v2"
    KAFKA_SERVER = 'localhost:9092'
    EXCHANGE_TOPIC = "coincap_exchanges"
    ASSETS_TOPIC = 'coincap_assets'
    # base_url = BASE_URL
    # kafka_server = KAFKA_SERVER
    # exchange_topic = EXCHANGE_TOPIC
    # assets_topic = ASSETS_TOPIC
    main(BASE_URL, EXCHANGE_TOPIC, ASSETS_TOPIC, KAFKA_SERVER)
