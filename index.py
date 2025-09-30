import base64
import os
import threading
import time
from enum import Enum
from typing import Dict, Any, Optional, List
import paho.mqtt.client as mqtt
import ssl
import json

import requests

from my_logger import logger

# version 0.11

CERTIFICATE_PATH = "rootCA.crt"

REGISTRY_ID = os.environ['REGISTRY_ID']
REGISTRY_PASSWORD = os.environ['REGISTRY_PASSWORD']

PUSHER_ID = "pusher"
PUSHER_DEVICE_ID = os.environ['PUSHER_DEVICE_ID']
PUSHER_PASSWORD = os.environ['PUSHER_DEVICE_PASSWORD']

TEST_PUSHER_ID = "pushernodemcu"
TEST_PUSHER_DEVICE_ID = os.environ['TEST_PUSHER_DEVICE_ID']
TEST_PUSHER_PASSWORD = os.environ['TEST_PUSHER_DEVICE_PASSWORD']


class RequestType(Enum):
    """Yandex Smart Home request types"""
    UNLINK = "unlink"
    DISCOVERY = "discovery"
    QUERY = "query"
    ACTION = "action"


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
            broker_address = "mqtt.cloud.yandex.net"
            broker_port = 8883

            logger.info(f"Connecting to MQTT broker at {broker_address}:{broker_port}")

            # Start loop in background thread
            self.client.loop_start()

            # Connect (non-blocking)
            self.client.connect(broker_address, broker_port, keepalive=60)

            # Wait for connection to establish
            if not self.connection_event.wait(timeout=10):
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


class DeviceManager:
    """Manages device configurations and operations"""

    def __init__(self, mqtt_client: ServerlessMQTTClient):
        self.registry_id = REGISTRY_ID
        self.registry_password = REGISTRY_PASSWORD
        self.mqtt_client = mqtt_client
        self.devices = {
            TEST_PUSHER_ID: {
                "mqtt_device_id": TEST_PUSHER_DEVICE_ID,
                "name": "nodemcuv2 Button Pusher",
                "password": TEST_PUSHER_PASSWORD,
                "description": "Smart button pusher device",
                "type": "devices.types.switch",
                "capabilities": ["devices.capabilities.on_off"]
            },
            PUSHER_ID: {
                "mqtt_device_id": PUSHER_DEVICE_ID,
                "name": "Button Pusher",
                "password": PUSHER_PASSWORD,
                "description": "Smart button pusher device",
                "type": "devices.types.switch",
                "capabilities": ["devices.capabilities.on_off"]
            }
        }

    def get_discovery_response(self, request_id: str) -> Dict[str, Any]:
        """Generate discovery response with all devices"""
        return {
            "request_id": request_id,
            "payload": {
                "user_id": "smart-home-user",
                "devices": [
                    {
                        "id": TEST_PUSHER_ID,
                        "name": "тестовый нажиматель",
                        "description": "Smart button pusher device",
                        "type": "devices.types.switch",
                        "capabilities": [
                            {
                                "type": "devices.capabilities.on_off",
                                "retrievable": True
                            }
                        ],
                        "device_info": {
                            "manufacturer": "NodeMCU",
                            "model": "NodeMCUV2",
                            "hw_version": "v1.0",
                            "sw_version": "pusher_0.2.0"
                        }
                    },
                    {
                        "id": PUSHER_ID,
                        "name": "нажиматель",
                        "description": "Smart button pusher device",
                        "type": "devices.types.switch",
                        "capabilities": [
                            {
                                "type": "devices.capabilities.on_off",
                                "retrievable": True
                            }
                        ],
                        "device_info": {
                            "manufacturer": "Wemos",
                            "model": "D1 Mini Lite ESP8266",
                            "hw_version": "v1.0",
                            "sw_version": "pusher_0.1.1"
                        }
                    }
                ]
            }
        }

    def get_query_response(self, request_id: str, devices: List[Dict], context) -> Dict[str, Any]:
        """Generate query response for device states"""
        response_devices = []

        for device in devices:
            device_id = device.get("id")
            mqtt_device_id = self.devices[device_id]["mqtt_device_id"]

            logger.info(f"get_query_response processing Device ID: {device_id}")

            value = False
            if device_id == TEST_PUSHER_ID or device_id == PUSHER_ID:
                # Request state
                publish_result = self.publish_command_to_api(device_id, "state", context)
                logger.info(f"Sent state request, result: {publish_result}")

                # Wait for state from MQTT
                state_data = self.mqtt_client.wait_for_state(mqtt_device_id, timeout=5.0)

                if state_data is not None:
                    if state_data.get('state') == "on":
                        value = True
                    logger.info(f"Got state for {device_id}: {state_data.get('state')}")
                else:
                    logger.warning(f"No state received for {device_id}")

            response_devices.append({
                "id": device_id,
                "capabilities": [
                    {
                        "type": "devices.capabilities.on_off",
                        "state": {
                            "instance": "on",
                            "value": value
                        }
                    }
                ]
            })

        return {
            "request_id": request_id,
            "payload": {
                "devices": response_devices
            }
        }

    def publish_command_to_api(self, device_id: str, message: str, context) -> bool:
        """Send MQTT command to device via Yandex IoT Core"""
        try:
            mqtt_device_id = self.devices[device_id]["mqtt_device_id"]
            url = f"https://iot-data.api.cloud.yandex.net/iot-devices/v1/registries/{self.registry_id}/publish"

            base64_message = base64.b64encode(message.encode('ascii')).decode('ascii')
            iam_token = context.token["access_token"]

            response = requests.post(
                url,
                headers={"Authorization": f"Bearer {iam_token}"},
                json={
                    "topic": f"$devices/{mqtt_device_id}/commands",
                    "data": base64_message
                }
            )

            if response.status_code == 200:
                logger.info(f"Successfully sent '{message}' to device {device_id}")
                return True
            else:
                logger.error(f"Failed to send command: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error publishing API command to {device_id}: {str(e)}")
            return False


class SmartHomeHandler:
    """Main handler for Yandex Smart Home requests"""

    def __init__(self, mqtt_client: ServerlessMQTTClient):
        self.device_manager = DeviceManager(mqtt_client)
        self.mqtt_client = mqtt_client

    def handle_unlink(self, request_id: str) -> Dict[str, Any]:
        """Handle account unlink request"""
        logger.info(f"Processing unlink request: {request_id}")
        return {"request_id": request_id}

    def handle_discovery(self, request_id: str) -> Dict[str, Any]:
        """Handle device discovery request"""
        logger.info(f"Processing discovery request: {request_id}")
        return self.device_manager.get_discovery_response(request_id)

    def handle_query(self, request_id: str, payload: Dict, context) -> Dict[str, Any]:
        """Handle device state query request"""
        logger.info(f"Processing query request: {request_id}")
        devices = payload.get("devices", [])
        return self.device_manager.get_query_response(request_id, devices, context)

    def handle_action(self, request_id: str, payload: Dict, context) -> Dict[str, Any]:
        """Handle device action request"""
        logger.info(f"Processing action request: {request_id}")
        devices = payload.get("devices", [])
        response_devices = []

        for device in devices:
            device_id = device.get("id")
            capabilities = device.get("capabilities", [])

            device_response = {
                "id": device_id,
                "capabilities": []
            }

            for capability in capabilities:
                capability_type = capability.get("type")
                capability_response = {
                    "type": capability_type,
                    "state": capability.get("state", {}).copy()
                }

                # Handle pusher device (on/off switch)
                if (device_id == PUSHER_ID or device_id == TEST_PUSHER_ID) and \
                        capability_type == "devices.capabilities.on_off":

                    state_value = capability["state"].get("value")
                    command = "1" if state_value else "0"
                    mqtt_device_id = self.device_manager.devices[device_id]["mqtt_device_id"]
                    expected_state = "on" if state_value else "off"

                    logger.info(f"Processing action for {device_id}, desired state: {expected_state}")

                    # Get current state before action
                    self.device_manager.publish_command_to_api(device_id, "state", context)
                    current_state_data = self.mqtt_client.wait_for_state(mqtt_device_id, timeout=3.0)
                    current_state = current_state_data.get('state') if current_state_data else None
                    logger.info(f"Current state before action: {current_state}")

                    # Perform the action
                    if self.device_manager.publish_command_to_api(device_id, command, context):
                        logger.info(f"Command '{command}' sent successfully")

                        # Request state after action
                        self.device_manager.publish_command_to_api(device_id, "state", context)

                        # Wait for state change (or new state)
                        state_after_data = self.mqtt_client.wait_for_state_change(
                            mqtt_device_id, current_state, timeout=5.0
                        )

                        # If no change detected, try getting any cached state
                        if state_after_data is None:
                            state_after_data = self.mqtt_client.get_cached_state(mqtt_device_id)

                        # Verify the action
                        if state_after_data is not None:
                            actual_state = state_after_data.get('state')

                            if actual_state == expected_state:
                                capability_response["state"]["action_result"] = {"status": "DONE"}
                                logger.info(f"Action verified: device is now {actual_state}")
                            else:
                                capability_response["state"]["action_result"] = {
                                    "status": "ERROR",
                                    "error_code": "DEVICE_UNREACHABLE",
                                    "error_message": f"Expected '{expected_state}' but got '{actual_state}'"
                                }
                                logger.error(f"Action failed: expected {expected_state}, got {actual_state}")
                        else:
                            capability_response["state"]["action_result"] = {
                                "status": "ERROR",
                                "error_code": "DEVICE_UNREACHABLE",
                                "error_message": "Failed to get state after action"
                            }
                            logger.error("No state received after action")
                    else:
                        capability_response["state"]["action_result"] = {
                            "status": "ERROR",
                            "error_code": "DEVICE_UNREACHABLE",
                            "error_message": "Failed to send command"
                        }
                        logger.error(f"Failed to send command to {device_id}")

                device_response["capabilities"].append(capability_response)

            response_devices.append(device_response)

        return {
            "request_id": request_id,
            "payload": {
                "devices": response_devices
            }
        }


def handler(event, context):
    """Main Yandex Cloud Function handler - invoked per request"""
    mqtt_client = None

    try:
        logger.info(f"Handler called with event: {json.dumps(event, indent=2)}")

        # Extract request parameters
        request_id = event["headers"]["request_id"]
        request_type = event["request_type"]
        payload = event.get("payload", {})

        logger.info(f"Processing request_type: {request_type}, request_id: {request_id}")

        # Initialize MQTT client and connect (only if needed)
        if request_type in [RequestType.QUERY.value, RequestType.ACTION.value]:
            mqtt_client = ServerlessMQTTClient(REGISTRY_ID, REGISTRY_PASSWORD)

            # Get all MQTT device IDs
            mqtt_device_ids = [PUSHER_DEVICE_ID, TEST_PUSHER_DEVICE_ID]

            if not mqtt_client.connect_and_subscribe(mqtt_device_ids):
                logger.error("Failed to establish MQTT connection")
                return {
                    "request_id": request_id,
                    "error_code": "INTERNAL_ERROR",
                    "error_message": "Failed to connect to MQTT broker"
                }

        # Initialize handler
        smart_home = SmartHomeHandler(mqtt_client) if mqtt_client else SmartHomeHandler(None)

        # Route request to appropriate handler
        if request_type == RequestType.UNLINK.value:
            result = smart_home.handle_unlink(request_id)

        elif request_type == RequestType.DISCOVERY.value:
            result = smart_home.handle_discovery(request_id)

        elif request_type == RequestType.QUERY.value:
            result = smart_home.handle_query(request_id, payload, context)

        elif request_type == RequestType.ACTION.value:
            result = smart_home.handle_action(request_id, payload, context)

        else:
            logger.error(f"Unknown request type: {request_type}")
            result = {
                "request_id": request_id,
                "error_code": "INVALID_ACTION",
                "error_message": "Unknown request type"
            }

        return result

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        return {
            "request_id": event.get("headers", {}).get("request_id", "unknown"),
            "error_code": "INTERNAL_ERROR",
            "error_message": str(e)
        }

    finally:
        # Always cleanup MQTT connection before function terminates
        if mqtt_client:
            mqtt_client.cleanup()
            logger.info("MQTT connection cleaned up")