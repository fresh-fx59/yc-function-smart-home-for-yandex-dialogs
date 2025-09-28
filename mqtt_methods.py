from my_logger import logger
import paho.mqtt.client as mqtt
import ssl
import time

# --- Configuration ---
MQTT_HOST = "mqtt.cloud.yandex.net"
MQTT_PORT = 8883
CERTIFICATE_PATH = os.environ['ROOT_CA']
CLIENT_CERT_PATH = os.environ['TEST_PUSHER_CLIENT_CERT']
CLIENT_KEY_PATH = os.environ['TEST_PUSHER_PRIVATE_KEY']
WAIT_TIME_SECONDS = 10


# --- Callback Functions (Keep these as they are) ---
def subscribe_and_wait(mqtt_topic: str):
    """
    Sets up the client, connects, subscribes to the given topic,
    and waits for a message for the defined timeout.
    """

    # Local variables to track message status
    message_received = False
    received_payload = None

    # --- Callback Functions Defined Locally ---
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"✅ Connected. Subscribing to: {mqtt_topic}...")
            # Use the topic passed to the outer function
            client.subscribe(mqtt_topic)
            logger.info(f"✅ Connected. Subscribed to: {mqtt_topic}")
        else:
            logger.error(f"❌ Connection failed with result code {rc}")

    def on_message(client, userdata, msg):
        nonlocal message_received, received_payload
        logger.info(f"📥 Message received on topic {msg.topic}")
        received_payload = msg.payload.decode()
        message_received = True

    # --- Client Setup (Same as before) ---
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    client.on_connect = on_connect
    client.on_message = on_message

    # ... (TLS and Connection setup code remains here) ...
    try:
        client.tls_set(
            ca_certs=CERTIFICATE_PATH,
            certfile=CLIENT_CERT_PATH,
            keyfile=CLIENT_KEY_PATH,
            tls_version=ssl.PROTOCOL_TLSv1_2
        )
    except FileNotFoundError:
        logger.error(f"Fatal Error: Certificate file not found at {CERTIFICATE_PATH}")
        return None

    try:
        client.connect(MQTT_HOST, MQTT_PORT, 60)
    except Exception as e:
        logger.error(f"Fatal Error: Could not connect to MQTT broker: {e}")
        return None

    # --- Loop and Cleanup (Same as before) ---
    client.loop_start()

    time.sleep(0.5)

    logger.info(f"⏳ Waiting for a message on '{mqtt_topic}' for {WAIT_TIME_SECONDS} seconds...")

    start_time = time.time()
    while time.time() - start_time < WAIT_TIME_SECONDS:
        if message_received:
            break
        time.sleep(0.1)

    client.loop_stop()
    client.disconnect()

    # --- Return the result ---
    if message_received:
        logger.info(f"Successfully retrieved payload from {mqtt_topic}.")
        return received_payload
    else:
        logger.info(f"Timeout reached. 💔 No message received on '{mqtt_topic}'.")
        return None
