import random
import json

order_id = 0


def get_order_details():
    global order_id
    order_id = order_id + 1
    sales_value = random.randint(10,1000)

    return json.dumps({"order_id" : order_id, "sales_value": sales_value})
        