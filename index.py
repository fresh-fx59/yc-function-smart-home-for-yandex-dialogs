import os

from my_logger import logger
from mqtt_methods import subscribe_and_wait_auth
import json
from enum import Enum
from typing import Dict, Any, List
import requests
import base64

# version 0.6 from github

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


class DeviceManager:
    """Manages device configurations and operations"""

    def __init__(self):
        self.registry_id = REGISTRY_ID
        self.registry_password = REGISTRY_PASSWORD
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
        # global result_from_topic, topic, device_id
        response_devices = []

        for device in devices:
            device_id = device.get("id")
            topic = self.getStateTopic(device_id)
            logger.info(f"get_query_response processing Device ID: {device_id}")

            result_from_topic = None
            if device_id == TEST_PUSHER_ID or device_id == PUSHER_ID:
                publish_result = self.publish_command_to_api(device_id, "state", context)
                logger.info(f"Got publish_result = {publish_result} from commands topic")

                result_from_topic = subscribe_and_wait_auth(topic, self.registry_id, self.registry_password)
                logger.info(f"Got result = {result_from_topic} from topic: {topic}")

            value = False
            logger.info(f"Same variables before if. Got result = {result_from_topic} from topic: {topic}")
            if result_from_topic is not None:
                if result_from_topic["state"] == "on":
                    value = True

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
                logger.info(f"Response body {response.text}")
                return True
            else:
                logger.error(f"Failed to send command to registry:{self.registry_id}:{device_id}:{mqtt_device_id} "
                             f" with iam token {iam_token}: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error publishing API command to {device_id}: {str(e)}")
            return False

    def getStateTopic(self, device_id):
        mqtt_device_id = self.devices[device_id]["mqtt_device_id"]
        return f"$devices/{mqtt_device_id}/state"


class SmartHomeHandler:
    """Main handler for Yandex Smart Home requests"""

    def __init__(self):
        self.device_manager = DeviceManager()

    def handle_unlink(self, request_id: str) -> Dict[str, Any]:
        """Handle account unlink request"""
        logger.info(f"Processing unlink request: {request_id}")
        # Here you could clean up user data, revoke tokens, etc.
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
                if (
                        device_id == PUSHER_ID or device_id == TEST_PUSHER_ID) and capability_type == "devices.capabilities.on_off":
                    state_value = capability["state"].get("value")
                    command = "1" if state_value else "0"
                    topic = self.device_manager.getStateTopic(device_id)

                    logger.info(f"Processing action for Device ID: {device_id}, desired state: {state_value}")

                    # Get state before action
                    logger.info(f"Getting state before action from topic: {topic}")
                    self.device_manager.publish_command_to_api(device_id, "state", context)
                    state_before = subscribe_and_wait_auth(topic, self.device_manager.registry_id,
                                                           self.device_manager.registry_password)
                    logger.info(f"State before action: {state_before}")

                    # Perform the action
                    if self.device_manager.publish_command_to_api(device_id, command, context):
                        logger.info(f"Command '{command}' sent successfully to device {device_id}")

                        # Get state after action
                        logger.info(f"Getting state after action from topic: {topic}")
                        self.device_manager.publish_command_to_api(device_id, "state", context)
                        state_after = subscribe_and_wait_auth(topic, self.device_manager.registry_id,
                                                              self.device_manager.registry_password)
                        logger.info(f"State after action: {state_after}")

                        # Verify the action completed successfully
                        if state_after is not None:
                            actual_state = state_after.get("state")
                            expected_state = "on" if state_value else "off"

                            if actual_state == expected_state:
                                capability_response["state"]["action_result"] = {"status": "DONE"}
                                logger.info(f"Action verified successful: device is now {actual_state}")
                            else:
                                capability_response["state"]["action_result"] = {
                                    "status": "ERROR",
                                    "error_code": "DEVICE_UNREACHABLE",
                                    "error_message": f"Expected state '{expected_state}' but got '{actual_state}'"
                                }
                                logger.error(
                                    f"Action verification failed: expected {expected_state}, got {actual_state}")
                        else:
                            capability_response["state"]["action_result"] = {
                                "status": "ERROR",
                                "error_code": "DEVICE_UNREACHABLE",
                                "error_message": "Failed to get state after action"
                            }
                            logger.error("Failed to retrieve state after action")
                    else:
                        capability_response["state"]["action_result"] = {
                            "status": "ERROR",
                            "error_code": "DEVICE_UNREACHABLE",
                            "error_message": "Failed to send command to device"
                        }
                        logger.error(f"Failed to send command to device {device_id}")

                device_response["capabilities"].append(capability_response)

            response_devices.append(device_response)

        return {
            "request_id": request_id,
            "payload": {
                "devices": response_devices
            }
        }


def handler(event, context):
    """Main Yandex Cloud Function handler"""
    try:
        logger.info(f"Handler called with event: {json.dumps(event, indent=2)}")

        # Extract request parameters
        request_id = event["headers"]["request_id"]
        request_type = event["request_type"]
        payload = event.get("payload", {})

        logger.info(f"Processing request_type: {request_type}, request_id: {request_id}")

        # Initialize handler
        smart_home = SmartHomeHandler()

        # Route request to appropriate handler
        if request_type == RequestType.UNLINK.value:
            return smart_home.handle_unlink(request_id)

        elif request_type == RequestType.DISCOVERY.value:
            return smart_home.handle_discovery(request_id)

        elif request_type == RequestType.QUERY.value:
            return smart_home.handle_query(request_id, payload, context)

        elif request_type == RequestType.ACTION.value:
            return smart_home.handle_action(request_id, payload, context)

        else:
            logger.error(f"Unknown request type: {request_type}")
            return {
                "request_id": request_id,
                "error_code": "INVALID_ACTION",
                "error_message": "Unknown request type"
            }

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        return {
            "request_id": event.get("headers", {}).get("request_id", "unknown"),
            "error_code": "INTERNAL_ERROR",
            "error_message": str(e)
        }
