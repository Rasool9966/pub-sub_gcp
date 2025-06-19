# pip install google-cloud-pubsub

import json
from google.cloud import pubsub_v1

# Initialize the Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient()

# Project and Topic details
project_id = "lyrical-respect-461712-k8"
subscription_name = "order_topic_sub_new"
subscription_path = subscriber.subscription_path(project_id, subscription_name)


# Pull and process messages
def pull_messages():
    while True:
        response = subscriber.pull(request={"subscription": subscription_path, "max_messages": 10})
        ack_ids = []

        for received_message in response.received_messages:
            # Extract JSON data
            json_data_str = received_message.message.data.decode('utf-8')
            
            # Deserialize the JSON data
            deserialized_data = json.loads(json_data_str)

            print(deserialized_data)
                      
            # Collect ack ID for acknowledgment
            ack_ids.append(received_message.ack_id)

        # Acknowledge the messages so they won't be sent again
        if ack_ids:
            subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})

# Run the consumer
if __name__ == "__main__":
    try:
        pull_messages()
    except KeyboardInterrupt:
        pass

