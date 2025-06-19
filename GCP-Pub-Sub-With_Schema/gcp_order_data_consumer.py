import io
import json
import avro.schema
from avro.io import DatumReader, BinaryDecoder
from google.pubsub_v1.services.schema_service import SchemaServiceClient
from google.pubsub_v1.types.schema import Schema, Encoding, GetSchemaRequest, SchemaView
from google.pubsub_v1.services.publisher import PublisherClient
from google.cloud.pubsub_v1 import SubscriberClient

PROJECT_ID      = "lyrical-respect-461712-k8"
SUBSCRIPTION_ID = "order_travel-sub"

def fetch_schema_and_encoding(project_id: str, subscription_id: str):
    """
    Discover schema_settings from the Topic attached to this Subscription,
    then fetch and parse the full Avro schema definition.
    """
    # 1) Find the Topic for our Subscription
    sub_client   = SubscriberClient()
    sub_path     = sub_client.subscription_path(project_id, subscription_id)
    subscription = sub_client.get_subscription(request={"subscription": sub_path})
    topic_path   = subscription.topic

    # 2) Read Topic metadata to get schema name & encoding
    pub_client  = PublisherClient()
    topic       = pub_client.get_topic(request={"topic": topic_path})
    schema_name = topic.schema_settings.schema
    encoding    = topic.schema_settings.encoding

    # 3) Fetch full schema definition
    schema_client = SchemaServiceClient()
    req = GetSchemaRequest(
        name=schema_name,
        view=SchemaView.FULL
    )
    schema_obj  = schema_client.get_schema(request=req)
    avro_schema = avro.schema.parse(schema_obj.definition)

    return avro_schema, encoding, sub_path

def main():
    # Fetch Avro schema, encoding, and the subscription path
    avro_schema, encoding, sub_path = fetch_schema_and_encoding(
        PROJECT_ID, SUBSCRIPTION_ID
    )

    subscriber = SubscriberClient()

    # Callback uses the discovered encoding & schema
    def callback(message):
        if encoding == Encoding.BINARY:
            buf     = io.BytesIO(message.data)
            decoder = BinaryDecoder(buf)
            reader  = DatumReader(avro_schema)
            record  = reader.read(decoder)
        elif encoding == Encoding.JSON:
            record = json.loads(message.data.decode("utf-8"))
        else:
            raise RuntimeError(f"Unsupported encoding: {encoding}")

        print("Received record:", record)
        message.ack()

    # Start streaming pull
    streaming_pull_future = subscriber.subscribe(sub_path, callback=callback)
    print(f"Listening for messages on {sub_path}...")

    # Block indefinitely (or until Ctrl+C)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("Listener stopped.")

if __name__ == "__main__":
    main()