import json
import time
import threading
from datetime import datetime, timedelta
from kafka import KafkaProducer
import random
import pytz

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Update if your Kafka broker is different (e.g., 'localhost:9092' or 'broker:29092' for Docker)
KAFKA_TOPIC = 'clickstream-events'

# Define users and their desired event publishing rate (events per minute)
USER_EVENT_RATES = {
    "userA": 5,  # 1 event per second
    "userB": 5,  # 2 events per second
    "userC": 5,  # 1 event every 2 seconds
    "userD": 5,  # 1.5 events per second
}

# --- Event Templates ---
# These templates will be used to generate events.
# The `timestamp` will be dynamically updated.
CLICK_EVENT_TEMPLATES = {
    "userA": [
        {"eventType": "page_view", "metadata": "{\"browser\":\"Chrome\"}", "eventData": "/home"},
        {"eventType": "click", "metadata": "{\"element\":\"button\"}", "eventData": "add_to_cart"},
        {"eventType": "scroll", "metadata": "{\"depth\":0.5}", "eventData": "product_page"},
        {"eventType": "purchase", "metadata": "{\"amount\":100}", "eventData": "item_id_123"},
    ],
    "userB": [
        {"eventType": "login", "metadata": "{\"method\":\"email\"}", "eventData": "success"},
        {"eventType": "click", "metadata": "{\"element\":\"button\"}", "eventData": "add_to_cart"},
        {"eventType": "click", "metadata": "{\"element\":\"foo\"}", "eventData": "bar"},
        {"eventType": "click", "metadata": "{\"element\":\"abc\"}", "eventData": "efg"},
        {"eventType": "click", "metadata": "{\"element\":\"ldd\"}", "eventData": "fff"},
    ],
    "userC": [
        {"eventType": "view_product", "metadata": "{\"product_id\":\"P101\"}", "eventData": "category_electronics"},
        {"eventType": "add_to_wishlist", "metadata": "{\"product_id\":\"P101\"}", "eventData": "wishlist_page"},
    ],
    "userD": [
        {"eventType": "search", "metadata": "{\"query\":\"laptop\"}", "eventData": "search_results"},
        {"eventType": "filter", "metadata": "{\"brand\":\"Dell\"}", "eventData": "filtered_results"},
        {"eventType": "sort", "metadata": "{\"by\":\"price_asc\"}", "eventData": "sorted_results"},
    ],
}


# --- Kafka Producer Function ---
def publish_user_events(user_id, events_per_minute, producer, topic, event_templates):
    """
    Publishes events for a specific user at a given rate.
    """
    events_per_second = events_per_minute / 60.0
    delay_per_event = 1.0 / events_per_second if events_per_second > 0 else 0

    print(
        f"[{user_id}] Starting to publish events at {events_per_minute} events/minute ({events_per_second:.2f} events/sec)")

    template_index = 0
    while True:
        # Get a random event template for the user
        if user_id in event_templates and event_templates[user_id]:
            template = random.choice(event_templates[user_id])
        else:
            print(f"[{user_id}] No event templates found. Skipping.")
            time.sleep(1)  # Avoid busy-waiting
            continue

        # Create a new event dictionary from the template
        event = template.copy()
        event['userId'] = user_id

        event['timestamp'] = int((datetime.now(pytz.timezone('Asia/Kolkata')) + timedelta(seconds=30)).timestamp() * 1000)

        # Convert event to JSON string (this is handled by value_serializer, so pass the dict directly)
        # event_json = json.dumps(event) # No longer needed here

        try:
            # Send the event to Kafka. Pass raw string for key, dict for value.
            producer.send(topic, key=user_id, value=event)
            print(f"[{user_id}] Published: {json.dumps(event)}")  # Print the JSON representation for readability
        except Exception as e:
            print(f"[{user_id}] Error publishing event: {e}")

        time.sleep(delay_per_event)
        template_index = (template_index + 1) % len(event_templates.get(user_id, [1]))  # Cycle through templates


# --- Main Execution ---
if __name__ == "__main__":
    print(f"Connecting to Kafka at: {KAFKA_BOOTSTRAP_SERVERS}")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        api_version=(0, 10, 2),  # Compatible with Kafka 0.10.2+
        retries=5,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )

    # Verify producer connection (optional, but good for debugging)
    try:
        # Send a dummy message to ensure connection is established
        # Pass raw string for key, dict for value.
        producer.send(KAFKA_TOPIC, key='test', value={"message": "producer_test"}).get(timeout=10)
        print("Kafka Producer connected successfully!")
    except Exception as e:
        print(f"Failed to connect to Kafka Producer: {e}")
        exit(1)

    threads = []
    for user_id, rate_per_minute in USER_EVENT_RATES.items():
        # Create a new thread for each user
        thread = threading.Thread(
            target=publish_user_events,
            args=(user_id, rate_per_minute, producer, KAFKA_TOPIC, CLICK_EVENT_TEMPLATES)
        )
        thread.daemon = True  # Allow main program to exit even if threads are running
        threads.append(thread)
        thread.start()

    print(f"Started {len(threads)} publisher threads. Press Ctrl+C to stop.")

    try:
        # Keep the main thread alive so daemon threads can run
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping publisher threads...")
        # No explicit thread stopping needed for daemon threads, they will exit with main.
    finally:
        producer.close()
        print("Kafka Producer closed.")
        print("Script finished.")
