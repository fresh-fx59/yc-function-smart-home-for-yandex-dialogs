from mqtt_client import MQTTClient
from my_logger import logger


def subscribe_and_wait_certs(mqtt_topic: str):
    """Public method to subscribe using client certificates."""
    try:
        client_wrapper = MQTTClient()
        client_wrapper.connect_certs()
        return client_wrapper.subscribe_and_wait(mqtt_topic)
    except Exception as e:
        logger.error(f"Subscription with certificates failed: {e}")
        return None


def subscribe_and_wait_auth(mqtt_topic: str, username: str, password: str):
    """Public method to subscribe using username/password (JWT)."""
    try:
        client_wrapper = MQTTClient()
        client_wrapper.connect_auth(username, password)
        return client_wrapper.subscribe_and_wait(mqtt_topic)
    except Exception as e:
        logger.error(f"Subscription with auth failed: {e}")
        return None