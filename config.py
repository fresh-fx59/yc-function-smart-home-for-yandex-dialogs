import os

MQTT_HOST = "mqtt.cloud.yandex.net"
MQTT_PORT = 8883
MQTT_KEEPALIVE = 1
MQTT_WAIT_FOR_STATE_TIMEOUT = 2
MQTT_WAIT_FOR_STATE_CHANGE_TIMEOUT = 2
MQTT_WAIT_FOR_CONNECTION_EVENT = 2

CERTIFICATE_PATH = "rootCA.crt"

REGISTRY_ID = os.environ['REGISTRY_ID']
REGISTRY_PASSWORD = os.environ['REGISTRY_PASSWORD']

PUSHER_ID = "pusher"
PUSHER_DEVICE_ID = os.environ['PUSHER_DEVICE_ID']
PUSHER_PASSWORD = os.environ['PUSHER_DEVICE_PASSWORD']

TEST_PUSHER_ID = "pushernodemcu"
TEST_PUSHER_DEVICE_ID = os.environ['TEST_PUSHER_DEVICE_ID']
TEST_PUSHER_PASSWORD = os.environ['TEST_PUSHER_DEVICE_PASSWORD']

WATERING_SYSTEM_ID = "watering-system"
WATERING_SYSTEM_DEVICE_ID = os.environ['WATERING_SYSTEM_DEVICE_ID']
WATERING_SYSTEM_PASSWORD = os.environ['WATERING_SYSTEM_DEVICE_PASSWORD']

DEVICES = {
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
    },
    WATERING_SYSTEM_ID: {
        "mqtt_device_id": WATERING_SYSTEM_DEVICE_ID,
        "name": "Plants Watering System",
        "password": WATERING_SYSTEM_PASSWORD,
        "description": "Plants Watering System just for launching watering process",
        "type": "devices.types.switch",
        "capabilities": ["devices.capabilities.on_off"]
    }
}
