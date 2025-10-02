import json
import ssl
import threading
import time
from typing import Optional, Dict, Any, List

import paho.mqtt.client as mqtt

from config import CERTIFICATE_PATH, MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE, MQTT_WAIT_FOR_CONNECTION_EVENT
from my_logger import logger


class ServerlessMQTTClient:
    """
    MQTT client for serverless environment
    Connects once per function invocation, collects all states, then disconnects
    """

    def __init__(self, registry_id: str, registry_password: str):
        self.registry_id = registry_id
        self.registry_password = registry_password
        self.device_states: Dict[str, Dict[str, Any]] = {}
        self.state_lock = threading.Lock()
        self.client: Optional[mqtt.Client] = None
        self.connected = False
        self.connection_event = threading.Event()

    def _on_connect(self, client, userdata, flags, rc):
        """Callback when connected to MQTT broker"""
        if rc == 0:
            logger.info("Successfully connected to MQTT broker")
            self.connected = True
            self.connection_event.set()
        else:
            logger.error(f"Failed to connect to MQTT broker with code: {rc}")
            self.connected = False
            self.connection_event.set()

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
                    self.device_states[device_id] = {
                        'state': payload.get('state'),
                        'timestamp': time.time(),
                        'raw_payload': payload
                    }
                    logger.info(f"Stored state for device {device_id}: {payload}")

        except Exception as e:
            logger.error(f"Error processing MQTT message: {str(e)}", exc_info=True)

    def connect_and_subscribe(self, mqtt_device_ids: List[str]) -> bool:
        """
        Connect to MQTT broker and subscribe to device topics
        This is called once at the start of function invocation
        """
        try:
            # Create MQTT client
            self.client = mqtt.Client(client_id=f"yandex_function_{int(time.time() * 1000)}")

            # Set callbacks
            self.client.on_connect = self._on_connect
            self.client.on_message = self._on_message

            # Set username/password
            self.client.username_pw_set(self.registry_id, self.registry_password)

            # Configure TLS/SSL
            self.client.tls_set(ca_certs=CERTIFICATE_PATH, tls_version=ssl.PROTOCOL_TLSv1_2)

            # Connect to Yandex IoT Core MQTT broker
            logger.info(f"Connecting to MQTT broker at {MQTT_HOST}:{MQTT_PORT}")

            # Start loop in background thread
            self.client.loop_start()

            # Connect (non-blocking)
            self.client.connect(MQTT_HOST, MQTT_PORT, keepalive=MQTT_KEEPALIVE)

            # Wait for connection to establish
            if not self.connection_event.wait(timeout=MQTT_WAIT_FOR_CONNECTION_EVENT):
                raise Exception("Connection timeout")

            if not self.connected:
                raise Exception("Failed to connect to MQTT broker")

            # Subscribe to all device state topics
            for mqtt_device_id in mqtt_device_ids:
                topic = f"$devices/{mqtt_device_id}/state"
                result = self.client.subscribe(topic)
                if result[0] == mqtt.MQTT_ERR_SUCCESS:
                    logger.info(f"Subscribed to topic: {topic}")
                else:
                    logger.error(f"Failed to subscribe to topic {topic}")

            logger.info("MQTT connection and subscription established successfully")
            return True

        except Exception as e:
            logger.error(f"Error connecting to MQTT broker: {str(e)}", exc_info=True)
            self.cleanup()
            return False

    def wait_for_state(self, mqtt_device_id: str, timeout: float = 5.0) -> Optional[Dict[str, Any]]:
        """
        Wait for state message from specific device
        """
        start_time = time.time()

        while (time.time() - start_time) < timeout:
            with self.state_lock:
                if mqtt_device_id in self.device_states:
                    logger.info(f"Got state for device {mqtt_device_id}")
                    return self.device_states[mqtt_device_id]

            time.sleep(0.1)  # Small delay to avoid busy waiting

        logger.warning(f"Timeout waiting for state from {mqtt_device_id}")
        return None

    def wait_for_state_change(self, mqtt_device_id: str, previous_state: Optional[str],
                              timeout: float = 5.0) -> Optional[Dict[str, Any]]:
        """
        Wait for state to change from previous state
        Useful for verifying action completion
        """
        start_time = time.time()

        while (time.time() - start_time) < timeout:
            with self.state_lock:
                if mqtt_device_id in self.device_states:
                    current_state = self.device_states[mqtt_device_id].get('state')
                    if current_state != previous_state:
                        logger.info(f"State changed for {mqtt_device_id}: {previous_state} -> {current_state}")
                        return self.device_states[mqtt_device_id]

            time.sleep(0.1)

        logger.warning(f"Timeout waiting for state change for {mqtt_device_id}")
        return None

    def get_cached_state(self, mqtt_device_id: str) -> Optional[Dict[str, Any]]:
        """Get cached state if available"""
        with self.state_lock:
            return self.device_states.get(mqtt_device_id)

    def cleanup(self):
        """Disconnect and cleanup - called at end of function invocation"""
        if self.client:
            try:
                self.client.loop_stop()
                self.client.disconnect()
                logger.info("MQTT client disconnected and cleaned up")
            except Exception as e:
                logger.error(f"Error during MQTT cleanup: {str(e)}")