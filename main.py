from confluent_kafka import Producer
import requests
from bs4 import BeautifulSoup
import time
import json


conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker adresi
}
producer = Producer(conf)


# Veriyi Kafka'ya göndermek için
def delivery_report(err, msg):
    if err is not None:
        print(f'Error: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def get_data_from_page():
    url = "https://scrapeme.live/shop/"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    products = soup.find_all('li', class_='product')
    data = []
    for product in products:
        name = product.find('h2', class_='woocommerce-loop-product__title').text
        price = product.find('span', class_='woocommerce-Price-amount').text

        # Açıklama elamanını kontrol eder
        description_element = product.find('div', class_='woocommerce-product-details__short-description')
        description = description_element.text.strip() if description_element else "No description available"

        # Stok elemanını kontrol eder
        stock_element = product.find('p', class_='stock')
        stock = stock_element.text.strip() if stock_element else "Stock information not available"

        data.append({'name': name, 'price': price, 'description': description, 'stock': stock})

    return data


def send_to_kafka():
    data = get_data_from_page()
    for item in data:
        producer.produce('DBrainTopic', value=json.dumps(item), callback=delivery_report)
        producer.poll(1)
        time.sleep(1)  # 1 saniye bekle


if __name__ == '__main__':
    send_to_kafka()
