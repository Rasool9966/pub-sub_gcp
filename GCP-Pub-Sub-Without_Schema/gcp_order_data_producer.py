import os
import random
import json
import time
from google.cloud import pubsub_v1



# Initialize the Pub/Sub publisher client

publisher = pubsub_v1.PublisherClient()




# Project and Topic details

project_id = "lyrical-respect-461712-k8"
topic_name ="order_topic"
topic_path = publisher.topic_path(project_id, topic_name)

def genrate_moke_data(order_id):
    
    items = ["Laptop", "Phone", "Book", "Tablet", "Monitor"]
    addresses = ["123 Main St, City A, Country", "456 Elm St, City B, Country", "789 Oak St, City C, Country"]
    statuses = ["Shipped", "Pending", "Delivered", "Cancelled"]

    return {
        "order_id"    : order_id,
        "customer_id" : random.randint(100,1000),
        "Item"        : random.choice(items),
        "quantity"    : random.randint(1,10),
        "price"       : random.uniform(100,1500),
        "Shipping_addres": random.choice(addresses),
        "order_status" : random.choice(statuses),
        "creation_date" : "2025-06-18"
    }

def callback(future):
    try:
        message_id = future.result()
        print(f"Published message with id : {message_id}")
    except Exception as e:
        print(f"Error publishing message: {e}")


order_id =1 

while True:

    data = genrate_moke_data(order_id)
    json_data = json.dumps(data).encode('utf-8')

    try:
        future = publisher.publish(topic_path, data=json_data)
        future.add_done_callback(callback)
        future.result()
    except Exception as e:
        print(f"Exception encountered: {e}")

    time.sleep(2)

    order_id += 1




