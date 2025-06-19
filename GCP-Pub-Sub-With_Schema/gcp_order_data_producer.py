# Install these libraries before running:
# pip install avro-python3
# pip install google-cloud-pubsub

import io
import time
import json
import uuid
import random
import datetime
import avro.schema
from avro.io import DatumWriter, BinaryEncoder

# Import high-level Publisher client from Google Cloud Pub/Sub
from google.cloud import pubsub_v1

# These are needed for schema management
from google.pubsub_v1.types.schema import Encoding, GetSchemaRequest, SchemaView
from google.pubsub_v1.services.schema_service import SchemaServiceClient

# Set GCP project and topic name
PROJECT_ID = 'lyrical-respect-461712-k8'
TOPIC_ID = 'order_travel'


# Function to fetch schema and encoding format used by the Pub/Sub topic
def fetch_topic_schema_encoding(project_id: str, topic_id: str):
    # Create a publisher client (high-level API)
    publisher = pubsub_v1.PublisherClient()
    
    # Build the fully qualified topic path
    topic_path = publisher.topic_path(project_id, topic_id)

    # Get the topic details (to retrieve the schema info)
    topic = publisher.get_topic(request={"topic": topic_path})

    # Get the schema name associated with the topic
    schema_name = topic.schema_settings.schema

    # Get the encoding type (either BINARY or JSON)
    encoding = topic.schema_settings.encoding

    # Create a schema client to fetch full schema definition
    schema_client = SchemaServiceClient()
    req = GetSchemaRequest(name=schema_name, view=SchemaView.FULL)
    schema_obj = schema_client.get_schema(request=req)

    # Parse the Avro schema string to an Avro schema object
    avro_schema = avro.schema.parse(schema_obj.definition)

    # Return all required components for publishing
    return avro_schema, encoding, topic_path


# Generate mock hotel booking data
def mock_booking() -> dict:
    today = datetime.date.today()
    checkin = today + datetime.timedelta(days=random.randint(1, 30))
    checkout = checkin + datetime.timedelta(days=random.randint(1, 7))

    return {
        "booking_id": str(uuid.uuid4()),
        "user_id": f"user-{random.randint(1000, 9999)}",
        "hotel_id": f"hotel-{random.randint(100, 199)}",
        "booking_date": today.isoformat(),
        "checkin_date": checkin.isoformat(),
        "checkout_date": checkout.isoformat(),
        "room_type": random.choice(["SINGLE", "DOUBLE", "SUITE"]),
        "amount": round(random.uniform(80, 500), 2),
        "currency": "USD",
        "status": random.choice(["CONFIRMED", "CANCELLED", "PENDING"]),
    }


# Convert a Python dict to Avro binary using the provided schema
def serialize_record(avro_schema, record: dict) -> bytes:
    buf = io.BytesIO()  # Create an in-memory byte buffer
    encoder = BinaryEncoder(buf)  # Create Avro encoder
    writer = DatumWriter(avro_schema)  # Create Avro writer using schema
    writer.write(record, encoder)  # Write the record into binary format
    return buf.getvalue()  # Return the bytes


# Entry point of the script
def main():
    # Fetch the Avro schema, encoding type, and topic path
    avro_schema, encoding, topic_path = fetch_topic_schema_encoding(PROJECT_ID, TOPIC_ID)

    # Create a publisher client (handles message publishing)
    publisher = pubsub_v1.PublisherClient()

    # Send 100 sample bookings
    for _ in range(100):
        rec = mock_booking()  # Generate fake booking record

        # Convert record to bytes, depending on encoding
        if encoding == Encoding.BINARY:
            data = serialize_record(avro_schema, rec)  # Use Avro binary serialization
        elif encoding == Encoding.JSON:
            data = json.dumps(rec).encode("utf-8")  # Convert to JSON-encoded bytes
        else:
            raise RuntimeError(f"Unsupported encoding: {encoding}")

        # Publish the message to Pub/Sub
        future = publisher.publish(topic_path, data=data)

        # Wait for message to be acknowledged and get its ID
        msg_id = future.result()

        # Log success
        print(f"✅ Published booking_id={rec['booking_id']} as msg_id={msg_id}")

        # Optional: delay between messages
        time.sleep(2)


# Run the producer script
if __name__ == "__main__":
    main()
