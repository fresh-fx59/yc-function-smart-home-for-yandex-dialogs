import base64
from typing import List, Dict, Any

import requests

from config import REGISTRY_ID, REGISTRY_PASSWORD, TEST_PUSHER_ID, \
    PUSHER_ID, MQTT_WAIT_FOR_STATE_TIMEOUT, MQTT_WAIT_FOR_STATE_CHANGE_TIMEOUT, \
    DEVICES, WATERING_SYSTEM_ID
from error_util import DEVICE_UNREACHABLE_CODE, get_error_response
from mqtt_client import ServerlessMQTTClient
from my_logger import logger


class DeviceManager:
    """Manages device configurations and operations"""

    def __init__(self, mqtt_client: ServerlessMQTTClient):
        self.registry_id = REGISTRY_ID
        self.registry_password = REGISTRY_PASSWORD
        self.mqtt_client = mqtt_client
        self.devices = DEVICES

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
                    },
                    {
                        "id": WATERING_SYSTEM_ID,
                        "name": "Plants Watering System",
                        "description": "Plants Watering System just for launching watering process",
                        "type": "devices.types.switch",
                        "capabilities": [
                            {
                                "type": "devices.capabilities.on_off",
                                "retrievable": False
                            }
                        ],
                        "device_info": {
                            "manufacturer": "Unknown",
                            "model": "YD-ESP32-23",
                            "hw_version": "2022-V1.3",
                            "sw_version": "ws_1.0.1"
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

            device_response = {
                "id": device_id
            }

            if (
                    device_id == TEST_PUSHER_ID
                    or device_id == PUSHER_ID
            ):
                # Request state
                publish_result = self.publish_command_to_api(device_id, "state", context)

                if not publish_result:
                    # Failed to send command to device
                    device_response["error_code"] = DEVICE_UNREACHABLE_CODE
                    device_response["error_message"] = "Failed to send state request to device"
                    logger.error(f"Failed to send state request to {device_id}")
                else:
                    logger.info(f"Sent state request, result: {publish_result}")

                    # Wait for state from MQTT
                    state_data = self.mqtt_client.wait_for_state(mqtt_device_id, timeout=MQTT_WAIT_FOR_STATE_TIMEOUT)

                    if state_data is not None:
                        value = True if state_data.get('state') == "on" else False
                        logger.info(f"Got state for {device_id}: {state_data.get('state')}")

                        device_response["capabilities"] = [
                            {
                                "type": "devices.capabilities.on_off",
                                "state": {
                                    "instance": "on",
                                    "value": value
                                }
                            }
                        ]
                    else:
                        # Device didn't respond
                        device_response["error_code"] = DEVICE_UNREACHABLE_CODE
                        device_response["error_message"] = "Device did not respond to state request"
                        logger.error(f"No state received for {device_id} within timeout")

            response_devices.append(device_response)

        return {
            "request_id": request_id,
            "payload": {
                "devices": response_devices
            }
        }

    def get_action_response(self, request_id: str, devices: List[Dict], context) -> Dict[str, Any]:
        """Generate action response"""
        response_devices = []

        for device in devices:
            device_id = device.get("id")
            capabilities = device.get("capabilities", [])

            device_response = {
                "id": device_id
            }

            device_capabilities = []

            for capability in capabilities:
                capability_type = capability.get("type")
                capability_response = {
                    "type": capability_type,
                    "state": capability.get("state", {}).copy()
                }

                # Handle pusher device (on/off switch)
                if (
                        device_id == PUSHER_ID
                        or device_id == TEST_PUSHER_ID
                        or device_id == WATERING_SYSTEM_ID
                ) and \
                        capability_type == "devices.capabilities.on_off":

                    state_value = capability["state"].get("value")
                    command = "1" if state_value else "0"
                    mqtt_device_id = self.devices[device_id]["mqtt_device_id"]
                    expected_state = "on" if state_value else "off"

                    logger.info(f"Processing action for {device_id}, desired state: {expected_state}")

                    # Get current state before action
                    if not self.publish_command_to_api(device_id, "state", context):
                        # Failed to send state request
                        device_capabilities.append(get_error_response(f"Failed to send state request to {device_id}", capability_response))
                        break

                    current_state_data = self.mqtt_client.wait_for_state(
                        mqtt_device_id, timeout=MQTT_WAIT_FOR_STATE_TIMEOUT)

                    if current_state_data is None:
                        # Device didn't respond to state request
                        device_capabilities.append(get_error_response(f"Device {device_id} didn't respond to state request", capability_response))
                        break

                    current_state = current_state_data.get('state')

                    logger.info(f"Current state before action: {current_state}")

                    # Perform the action
                    if not self.publish_command_to_api(device_id, command, context):
                        # Failed to send action command
                        device_capabilities.append(get_error_response(f"Failed to send command to {device_id}", capability_response))
                    else:
                        logger.info(f"Command '{command}' sent successfully")

                        # Request state after action
                        if not self.publish_command_to_api(device_id, "state", context):
                            device_capabilities.append(
                                get_error_response(f"Failed to request state after action from {device_id}", capability_response))

                        else:
                            # Wait for state change (or new state)
                            state_after_data = self.mqtt_client.wait_for_state_change(
                                mqtt_device_id, current_state, timeout=MQTT_WAIT_FOR_STATE_CHANGE_TIMEOUT
                            )

                            # If no change detected, try getting any cached state
                            if state_after_data is None:
                                state_after_data = self.mqtt_client.get_cached_state(mqtt_device_id)

                            # Verify the action
                            if state_after_data is not None:
                                actual_state = state_after_data.get('state')

                                if actual_state == expected_state:
                                    capability_response["state"]["action_result"] = {"status": "DONE"}
                                    device_capabilities.append(capability_response)
                                    logger.info(f"Action verified: device is now {actual_state}")
                                else:
                                    device_capabilities.append(
                                        get_error_response(f"Action failed: expected {expected_state}, got {actual_state}",
                                                           capability_response))
                            else:
                                device_capabilities.append(
                                    get_error_response(f"No state received from {device_id} after action", capability_response))
                                break

            # Set device-level response
            device_response["capabilities"] = device_capabilities
            response_devices.append(device_response)

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
