

## Project Structure

**Core Components:**
- `index.py` - Main entry point and request handler for Yandex Cloud Function
- `device_manager.py` - Device orchestration (discovery, query, action handling)
- `mqtt_client.py` - MQTT client for communication with IoT devices
- `config.py` - Configuration and device registry
- `error_util.py` - Error handling utilities
- `my_logger.py` - Custom JSON logging for Yandex Cloud

## Supported Devices

Currently supports:
- **Smart button pushers** (on/off switches with state verification)
- **Watering system** (fire-and-forget commands)

## Request Types

The function handles four types of Yandex Smart Home requests:
- **DISCOVERY** - Returns list of available devices
- **QUERY** - Queries current device state via MQTT
- **ACTION** - Executes device actions and verifies state changes
- **UNLINK** - Account unlinking (not yet implemented)

## Deployment

Automated deployment via GitHub Actions:
- **Production**: Pushes to `main` branch → production environment
- **Preprod**: Pushes to `feature**` branches → preprod environment

Deployment uses `yc-actions/yc-sls-function@v3.1.0` action.

## Configuration

Required environment variables (set via Yandex Cloud Secrets):
- `REGISTRY_ID`, `REGISTRY_PASSWORD` - IoT Core registry credentials
- `PUSHER_DEVICE_ID`, `TEST_PUSHER_DEVICE_ID`, `WATERING_SYSTEM_DEVICE_ID` - MQTT device IDs
- Device passwords for each device

Required files:
- `rootCA.crt` - TLS certificate for MQTT connection

## Service Accounts

Two service accounts required:
1. **Deployment account** - functions.admin, iam.serviceAccounts.user, logging.editor, vpc.user
2. **Runtime account** - lockbox.payloadViewer, iot.registries.writer, iot.devices.writer, kms.keys.encrypterDecrypter

For more details, see [CLAUDE.md](CLAUDE.md).

## Useful links

[yandex smart home api](https://yandex.ru/dev/dialogs/smart-home/doc/en/reference/post-devices-query-jrpc) is used to communicate between smart home elements

[yandex dialogs platform](https://dialogs.yandex.ru/developer) allow us to connect yandex function with yandex smart home

I use [platformio](https://platformio.org/platformio-ide) with vscode to develop firmware on C++ for my devices.

You need to be registered on [yandex cloud](https://console.yandex.cloud/), payment method added.

[smart home with Alice app](https://play.google.com/store/apps/details?id=com.yandex.iot&hl=en)

## Commands flow
### Watering the flowers command flow

- we push the button in the yandex home with Alice mobile app
- it sends the request to yandex cloud serverless function
- serverless function sends request to device
- if request successful, then we consider command executed
- if request failed, function send appropriate message to the app
- device get request and start watering the flowers

### Pusher command flow

- we push the button in the yandex home with Alice mobile app
- it sends the request to yandex cloud serverless function
- function subscribes to state mqtt topic
- serverless function sends request to device and waiting for response in state topic
- request successful, if state changed received
- request failed, if state changed wasn't received
- device get request, execute action and sent state change to state topic
