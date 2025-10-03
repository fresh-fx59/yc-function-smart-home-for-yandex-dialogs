import json
from typing import Dict, Any

from config import REGISTRY_ID, REGISTRY_PASSWORD, DEVICES
from device_manager import DeviceManager
from error_util import create_error_response
from model import RequestType
from mqtt_client import ServerlessMQTTClient
from my_logger import logger


# version 0.15.7

class SmartHomeHandler:
    """Main handler for Yandex Smart Home requests"""

    def __init__(self, mqtt_client: ServerlessMQTTClient):
        self.device_manager = DeviceManager(mqtt_client)
        self.mqtt_client = mqtt_client

    def handle_unlink(self, request_id: str) -> Dict[str, Any]:
        """Handle account unlink request"""
        logger.info(f"Processing unlink request: {request_id}")
        # todo: implement unlink
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
        return self.device_manager.get_action_response(request_id, devices, context)


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
            mqtt_device_ids = []
            for device in payload.get("devices", []):
                mqtt_device_ids.append(DEVICES[device.get("id")]["mqtt_device_id"])

            if not mqtt_client.connect_and_subscribe(mqtt_device_ids):
                logger.error("Failed to establish MQTT connection")
                return create_error_response(
                    event,
                    error_message=f"Failed to subscribe at device_ids({mqtt_device_ids}) topics for request = {request_type}"
                )

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
