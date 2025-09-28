from my_logger import logger
from mqtt_methods import subscribe_and_wait
import json
from enum import Enum
from typing import Dict, Any, List
import requests
import base64
from dataclasses import dataclass, field

# version 0.1.2 from github

PUSHER_ID = "pusher"
TEST_PUSHER_ID = "pushernodemcu"
PUSHER_DEVICE_ID = os.environ['PUSHER_DEVICE_ID']
TEST_PUSHER_DEVICE_ID = os.environ['TEST_PUSHER_DEVICE_ID']
REGISTRY_ID = os.environ['REGISTRY_ID']


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
        self.devices = {
            TEST_PUSHER_ID: {
                "mqtt_device_id": TEST_PUSHER_DEVICE_ID,
                "name": "nodemcuv2 Button Pusher",
                "description": "Smart button pusher device",
                "type": "devices.types.switch",
                "capabilities": ["devices.capabilities.on_off"]
            },
            PUSHER_ID: {
                "mqtt_device_id": PUSHER_DEVICE_ID,
                "name": "Button Pusher",
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
                                "retrievable": False
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
                                "retrievable": False
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

        # for device in devices:
        #     device_id = device.get("id")

        #     result_from_topic = None
        #     if (device_id == TEST_PUSHER_ID or device_id == PUSHER_ID):
        # topic = self.getStateTopic(device_id);
        # publish_result = self.publish_command_to_api(device_id, "state", context)
        # logger.info(f"Got publish_result = {publish_result} from commands topic");
        # result_from_topic = subscribe_and_wait(topic)
        # logger.info(f"Got result = {result_from_topic} from topic: {topic}");

        # value = False
        # logger.info(f"Same variables before if. Got result = {result_from_topic} from topic: {topic}");
        # if result_from_topic != None:
        #     if result_from_topic["state"] == "on":
        #         value = True

        # response_devices.append({
        #     "id": device_id,
        #     "capabilities": [
        #         {
        #             "type": "devices.capabilities.on_off",
        #             "state": {
        #                 "instance": "on",
        #                 "value": value
        #             }
        #         }
        #     ]
        # })

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
                logger.error(f"Failed to send command to {device_id}: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error publishing API command to {device_id}: {str(e)}")
            return False

    def getStateTopic(self, device_id):
        mqtt_device_id = self.devices[device_id]["mqtt_device_id"]
        return f"$devices/{mqtt_device_id}/events/"


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

            success = True

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

                    if self.device_manager.publish_command_to_api(device_id, command, context):
                        capability_response["state"]["action_result"] = {"status": "DONE"}
                        logger.info(f"Pusher command sent successfully: {command}")
                    else:
                        capability_response["state"]["action_result"] = {
                            "status": "ERROR",
                            "error_code": "DEVICE_UNREACHABLE"
                        }
                        success = False

                device_response["capabilities"].append(capability_response)

            # Add overall device result if needed
            if not success:
                device_response["action_result"] = {
                    "status": "ERROR",
                    "error_code": "DEVICE_UNREACHABLE"
                }

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
