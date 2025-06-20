# import base64
# import functions_framework

# # Triggered from a message on a Cloud Pub/Sub topic.
# @functions_framework.cloud_event
# def hello_pubsub(cloud_event):
#     # Print out the data from Pub/Sub, to prove that it worked
#     # print(base64.b64decode(cloud_event.data["message"]["data"]))

import base64
import json
import logging
from datetime import datetime, timezone
import functions_framework

# Configure logging
logging.basicConfig(level=logging.INFO)

@functions_framework.cloud_event
def process_order_event(cloud_event):
    """
    Cloud Run handler triggered by Pub/Sub.
    Expects the Pub/Sub message to be a base64-encoded JSON with fields:
      order_id, customer_id, item, quantity, price,
      shipping_address, order_status, creation_date
    This function:
      - decodes & parses the JSON
      - computes total_amount = quantity * price
      - uppercases order_status
      - adds processed_at UTC timestamp
      - logs the enriched payload
    """

    # 1) Extract & decode the Pub/Sub payload
    msg = cloud_event.data.get("message", {})
    raw = msg.get("data", "")
    if not raw:
        logging.error("No data payload in Pub/Sub message.")
        return ("No data", 400)

    try:
        decoded = base64.b64decode(raw).decode("utf-8")
        order = json.loads(decoded)
    except Exception as e:
        logging.exception("Failed to decode/parse Pub/Sub message")
        return (f"Bad message format: {e}", 400)

    # 2) Validate required fields
    required = ["order_id", "quantity", "price", "order_status"]
    missing  = [f for f in required if f not in order]
    if missing:
        logging.error(f"Missing required fields: {missing}")
        return (f"Missing fields: {missing}", 400)

    # 3) Compute derived/enriched fields
    try:
        qty   = float(order["quantity"])
        price = float(order["price"])
        order["total_amount"] = round(qty * price, 2)
    except Exception:
        logging.exception("Error computing total_amount")
        order["total_amount"] = None

    # 4) Normalize status & timestamps
    order["order_status"] = order["order_status"].strip().upper()
    order["processed_at"] = datetime.now(timezone.utc).isoformat()

    # 5) (Optional) Additional business logic:
    #    e.g. route to different Pub/Sub topic based on status,
    #         enrich address via Geocoding API, etc.

    # 6) Log the final enriched order for visibility
    logging.info(f"Enriched order event: {json.dumps(order)}")

    # 7) Return for local testing; Cloud Run will return 200 OK by default
    return (json.dumps(order), 200)