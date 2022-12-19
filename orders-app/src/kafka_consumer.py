from kafka import KafkaConsumer
from config import TOPIC, BOOTSTRAP_SERVER, GROUP_ID, ZINCSEARCH_SERVER, INDEX
import base64, json
import requests


# To consume latest messages from the given topic and auto-commit offsets
consumer = KafkaConsumer(TOPIC,
                         group_id=GROUP_ID,
                         bootstrap_servers=BOOTSTRAP_SERVER,
                         auto_offset_reset='earliest')

user = "admin"
password = "admin"
bas64encoded_creds = base64.b64encode(bytes(user + ":" + password, "utf-8")).decode("utf-8")
headers = {"Content-type": "application/json", "Authorization": "Basic " + bas64encoded_creds}
ZINC_URL = ZINCSEARCH_SERVER + "api/" + INDEX + "/_doc"

for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                         message.offset, message.key.decode("utf-8"),
                                         message.value.decode("utf-8")))

    print(type(message.value))
    print(type(message.value.decode("utf-8")))
    data = message.value.decode("utf-8")

    requests.post(ZINC_URL, headers=headers, data=data)

