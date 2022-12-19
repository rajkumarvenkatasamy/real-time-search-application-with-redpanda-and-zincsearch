import time

from kafka import KafkaProducer

from config import BOOTSTRAP_SERVER, TOPIC
import orders


def string_serializer(data):
    return str(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER,
                         value_serializer=string_serializer, key_serializer=string_serializer)


if __name__ == "__main__":

    while True:
        message_key = 0
        message = orders.get_order_details()
        print(message)
        producer.send(topic=TOPIC, value=message, key=message_key)
        message_key = message_key+1
        time.sleep(3)

        # Wait until all async messages are sent
        producer.flush()
