import sys
import time
import requests
from kafka import KafkaProducer
from json import dumps
import json


def fetch_write_topic(url: str, producer: KafkaProducer, topic: str) -> bool:
    try:
        response = requests.get(url)
        if response.status_code == 200:
            producer.send(topic, response.json())
            return True
        else:
            return False
    except requests.exceptions.RequestException as e:
        print('Connection error!')
        return False


def print_in_line(text: str):
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
    main(BASE_URL, EXCHANGE_TOPIC, ASSETS_TOPIC, KAFKA_SERVER)
