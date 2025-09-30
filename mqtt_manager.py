import os
import threading
import time
from typing import Dict, Any, Optional
import paho.mqtt.client as mqtt
import ssl
import json

from my_logger import logger

REGISTRY_ID = os.environ['REGISTRY_ID']
REGISTRY_PASSWORD = os.environ['REGISTRY_PASSWORD']

PUSHER_ID = "pusher"
PUSHER_DEVICE_ID = os.environ['PUSHER_DEVICE_ID']

TEST_PUSHER_ID = "pushernodemcu"
TEST_PUSHER_DEVICE_ID = os.environ['TEST_PUSHER_DEVICE_ID']

MQTT_HOST = "mqtt.cloud.yandex.net"
MQTT_PORT = 8883

STATE_MAX_AGE = 30.0
STATE_UPDATE_TIMEOUT = 5.0
CONNECTION_KEEP_ALIVE = 60
TIMEOUT_WAITING_FOR_CONNECTION = 10
SMALL_DELAY = 0.1


class MQTTStateManager:
    """Manages persistent MQTT connection and device states"""

    def __init__(self, registry_id: str, registry_password: str):
        self.registry_id = registry_id
        self.registry_password = registry_password
        self.device_states: Dict[str, Dict[str, Any]] = {}
        self.state_lock = threading.Lock()
        self.client: Optional[mqtt.Client] = None
        self.connected = False
        self.subscribed_topics = set()

    def _on_connect(self, client, userdata, flags, rc):
        """Callback when connected to MQTT broker"""
        if rc == 0:
            logger.info("Successfully connected to MQTT broker")
            self.connected = True
            # Resubscribe to topics after reconnection
            for topic in self.subscribed_topics:
                client.subscribe(topic)
                logger.info(f"Resubscribed to topic: {topic}")
        else:
            logger.error(f"Failed to connect to MQTT broker with code: {rc}")
            self.connected = False

    def _on_disconnect(self, client, userdata, rc):
        """Callback when disconnected from MQTT broker"""
        logger.warning(f"Disconnected from MQTT broker with code: {rc}")
        self.connected = False
        if rc != 0:
            logger.info("Unexpected disconnection, will attempt to reconnect")

    def _on_message(self, client, userdata, msg):
        """Callback when message received from MQTT topic"""
        try:
            payload = json.loads(msg.payload.decode('utf-8'))
            topic = msg.topic

            logger.info(f"Received message from topic {topic}: {payload}")

            # Extract device_id from topic: $devices/{device_id}/state
            if '/state' in topic:
                device_id = topic.split('/')[1]

                with self.state_lock:
                    # Store the state with timestamp
                    self.device_states[device_id] = {
                        'state': payload.get('state'),
                        'timestamp': time.time(),
                        'raw_payload': payload
                    }
                    logger.info(f"Updated state for device {device_id}: {payload}")

        except Exception as e:
            logger.error(f"Error processing MQTT message: {str(e)}", exc_info=True)

    def connect(self):
        """Establish persistent MQTT connection"""
        try:
            # Create MQTT client
            self.client = mqtt.Client(client_id=f"yandex_function_{int(time.time())}")

            # Set callbacks
            self.client.on_connect = self._on_connect
            self.client.on_disconnect = self._on_disconnect
            self.client.on_message = self._on_message

            # Set username/password
            self.client.username_pw_set(self.registry_id, self.registry_password)

            # Configure TLS/SSL
            self.client.tls_set(ca_certs=None, certfile=None, keyfile=None,
                                cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2)

            # Connect to Yandex IoT Core MQTT broker
            logger.info(f"Connecting to MQTT broker at {MQTT_HOST}:{MQTT_PORT}")
            self.client.connect(MQTT_HOST, MQTT_PORT, keepalive=CONNECTION_KEEP_ALIVE)

            # Start network loop in background thread
            self.client.loop_start()

            # Wait for connection to establish
            start_time = time.time()
            while not self.connected and (time.time() - start_time) < TIMEOUT_WAITING_FOR_CONNECTION:
                time.sleep(SMALL_DELAY)

            if not self.connected:
                raise Exception("Failed to connect to MQTT broker within timeout")

            logger.info("MQTT connection established successfully")
            return True

        except Exception as e:
            logger.error(f"Error connecting to MQTT broker: {str(e)}", exc_info=True)
            return False

    def subscribe_to_device(self, mqtt_device_id: str):
        """Subscribe to device state topic"""
        try:
            topic = f"$devices/{mqtt_device_id}/state"

            if self.client and self.connected:
                result = self.client.subscribe(topic)
                if result[0] == mqtt.MQTT_ERR_SUCCESS:
                    self.subscribed_topics.add(topic)
                    logger.info(f"Subscribed to topic: {topic}")
                    return True
                else:
                    logger.error(f"Failed to subscribe to topic {topic}: {result}")
                    return False
            else:
                logger.error("Cannot subscribe: MQTT client not connected")
                return False

        except Exception as e:
            logger.error(f"Error subscribing to device {mqtt_device_id}: {str(e)}", exc_info=True)
            return False

    def get_device_state(self, mqtt_device_id: str, max_age_seconds: float = STATE_MAX_AGE) -> Optional[Dict[str, Any]]:
        """
        Get cached device state

        Args:
            mqtt_device_id: MQTT device identifier
            max_age_seconds: Maximum age of cached state in seconds (default 30)

        Returns:
            Device state dict or None if not available or too old
        """
        with self.state_lock:
            if mqtt_device_id in self.device_states:
                state_data = self.device_states[mqtt_device_id]
                age = time.time() - state_data['timestamp']

                if age <= max_age_seconds:
                    logger.info(f"Retrieved cached state for {mqtt_device_id} (age: {age:.2f}s)")
                    return state_data
                else:
                    logger.warning(f"Cached state for {mqtt_device_id} is too old ({age:.2f}s)")
                    return None
            else:
                logger.info(f"No cached state available for {mqtt_device_id}")
                return None

    def wait_for_state_update(self, mqtt_device_id: str, timeout: float = STATE_UPDATE_TIMEOUT) -> Optional[Dict[str, Any]]:
        """
        Wait for a new state update for a device

        Args:
            mqtt_device_id: MQTT device identifier
            timeout: Maximum time to wait in seconds

        Returns:
            Updated device state or None if timeout
        """
        start_time = time.time()
        initial_timestamp = None

        # Get initial timestamp if state exists
        with self.state_lock:
            if mqtt_device_id in self.device_states:
                initial_timestamp = self.device_states[mqtt_device_id]['timestamp']

        # Wait for state update
        while (time.time() - start_time) < timeout:
            with self.state_lock:
                if mqtt_device_id in self.device_states:
                    current_timestamp = self.device_states[mqtt_device_id]['timestamp']
                    # Check if we got a new update
                    if initial_timestamp is None or current_timestamp > initial_timestamp:
                        logger.info(f"Received state update for {mqtt_device_id}")
                        return self.device_states[mqtt_device_id]

            time.sleep(SMALL_DELAY)  # Small delay to avoid busy waiting

        logger.warning(f"Timeout waiting for state update for {mqtt_device_id}")
        return None

    def disconnect(self):
        """Disconnect from MQTT broker"""
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("Disconnected from MQTT broker")


# Global instance
_mqtt_manager: Optional[MQTTStateManager] = None


def get_mqtt_manager() -> MQTTStateManager:
    """Get or create global MQTT manager instance"""
    global _mqtt_manager

    if _mqtt_manager is None:
        logger.info("Initializing MQTT State Manager")
        _mqtt_manager = MQTTStateManager(REGISTRY_ID, REGISTRY_PASSWORD)

        if _mqtt_manager.connect():
            # Subscribe to all device state topics
            _mqtt_manager.subscribe_to_device(PUSHER_DEVICE_ID)
            _mqtt_manager.subscribe_to_device(TEST_PUSHER_DEVICE_ID)
            logger.info("MQTT State Manager initialized and subscribed to devices")
        else:
            logger.error("Failed to initialize MQTT State Manager")

    return _mqtt_manager
