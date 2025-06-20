# main.py

import os
import uuid
import random
import json
import logging
from datetime import datetime, timedelta
import functions_framework
from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO)

# Topic setup via env var
PROJECT_ID = "mythic-aloe-457912-d5"
TOPIC_ID   = "movie-bookings"
publisher  = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# Mock pools
MOVIES     = [
    "The Grand Heist", "Future Worlds", "Love in Autumn",
    "Mystery Manor", "Comedy Nights", "Space Odyssey"
]
THEATERS   = ["Cineplex 1", "Galaxy Cinema", "Starlight Theater", "Downtown Screens"]
CURRENCIES = ["USD","EUR","GBP","INR","JPY"]
STATUSES   = ["CONFIRMED","CANCELLED","PENDING"]

@functions_framework.http
def generate_movie_bookings(request):

    logging.info(f"Received PING from scheduled cron job !!")
    count = random.randint(3,7)
    now   = datetime.utcnow()
    for _ in range(count):
        show_time = now + timedelta(hours=random.randint(1,72))
        booking = {
            "booking_id":    str(uuid.uuid4()),
            "user_id":       f"user_{random.randint(1000,9999)}",
            "movie_title":   random.choice(MOVIES),
            "theater_name":  random.choice(THEATERS),
            "show_time":     show_time.isoformat(),
            "seat":          f"{random.choice('ABCDEF')}{random.randint(1,20)}",
            "ticket_price":  round(random.uniform(8.0,25.0), 2),
            "currency":      random.choice(CURRENCIES),
            "booking_status":random.choice(STATUSES),
            "booked_at":     now.isoformat()
        }
        data = json.dumps(booking).encode("utf-8")
        future = publisher.publish(topic_path, data)
        logging.info(f"Published movie booking {booking['booking_id']} → msg_id={future.result()}")
    return (f"Published {count} movie bookings\n", 200)