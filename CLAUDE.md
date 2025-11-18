# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Yandex Cloud serverless function that implements the Yandex Smart Home API for controlling IoT devices (smart switches, watering systems) via Yandex Alice. The function acts as a bridge between Yandex Smart Home and physical devices connected through Yandex IoT Core MQTT broker.

**Current version**: 0.16.2 (see index.py:12)

**Key integration points**:
- Yandex Smart Home API: https://yandex.ru/dev/dialogs/smart-home/doc/en/reference/post-devices-query-jrpc
- Yandex Dialogs Platform: https://dialogs.yandex.ru/developer
- Yandex Cloud Console: https://console.yandex.cloud/

## Architecture

### Request Flow
1. User interacts with Yandex Smart Home app (Alice)
2. Request sent to Yandex Cloud
3. Serverless function loads and processes request
4. Function publishes MQTT command to device via Yandex IoT Core REST API
5. Function subscribes to device's MQTT state topic
6. Device receives command and publishes state to MQTT topic
7. Function receives state response and returns to Yandex Smart Home API

### Core Components

**index.py** - Main entry point
- `handler(event, context)`: Yandex Cloud Function entry point (invoked per request)
- `SmartHomeHandler`: Routes requests to appropriate handlers
- Handles 4 request types: UNLINK, DISCOVERY, QUERY, ACTION (see model.py)
- Initializes MQTT connection only for QUERY and ACTION requests

**device_manager.py** - Device orchestration
- `get_discovery_response()`: Returns list of available devices for Yandex Smart Home
- `get_query_response()`: Queries device state via MQTT
- `get_action_response()`: Executes device actions and verifies state changes
- `publish_command_to_api()`: Publishes commands via Yandex IoT Core REST API (not direct MQTT)

**mqtt_client.py** - MQTT communication
- `ServerlessMQTTClient`: Connects once per function invocation, collects states, then disconnects
- Uses threading to handle async MQTT messages
- Implements retry logic (3 attempts with exponential backoff) for connection stability
- `wait_for_state()`: Polls for device state with timeout
- `wait_for_state_change()`: Waits for state to change after action (for verification)
- Subscribes to `$devices/{device_id}/state` topics
- Commands sent via REST API to `$devices/{device_id}/commands` topics

**config.py** - Configuration
- Loads device credentials from environment variables
- Device registry in `DEVICES` dict maps Yandex device IDs to MQTT device IDs
- MQTT connection timeouts and keepalive settings

### Device Types

Currently supports:
1. **Pusher devices** (TEST_PUSHER_ID, PUSHER_ID): On/off switches that verify state changes
2. **Watering system** (WATERING_SYSTEM_ID): Fire-and-forget command (no state verification)

When adding new devices:
- Add config to `DEVICES` dict in config.py
- Add environment variables for device ID and password
- Add device definition in `get_discovery_response()` in device_manager.py
- Add action/query logic in respective methods if custom behavior needed

### State Verification Pattern

For devices with retrievable state (pushers):
1. Request current state before action
2. Send action command
3. Request state after action
4. Wait for state change confirmation
5. Verify actual state matches expected state

For non-retrievable devices (watering system):
- Send command and immediately return success (capability.retrievable = False)

## Development Commands

**No testing framework is currently set up** - this is production code with no unit tests.

### Deployment

Deployment happens via GitHub Actions (see .github/workflows/):

**CI (feature branches)**: Deploys to preprod environment
```bash
# Auto-triggers on push to feature** branches
```

**CD (main branch)**: Deploys to production
```bash
# Auto-triggers on push to main
# Can also be manually triggered via GitHub Actions UI
```

The deployment uses `yc-actions/yc-sls-function@v3.1.0` to deploy to Yandex Cloud.

### Version Updates

Update version number in index.py:12 when making changes.

## Environment Configuration

Required environment variables (set via Yandex Cloud Secrets):
- `REGISTRY_ID`: Yandex IoT Core registry ID
- `REGISTRY_PASSWORD`: Registry password for MQTT auth
- `PUSHER_DEVICE_ID`, `TEST_PUSHER_DEVICE_ID`, `WATERING_SYSTEM_DEVICE_ID`: MQTT device IDs
- `PUSHER_DEVICE_PASSWORD`, `TEST_PUSHER_DEVICE_PASSWORD`, `WATERING_SYSTEM_DEVICE_PASSWORD`: Device passwords

Required files:
- `rootCA.crt`: TLS certificate for Yandex IoT Core MQTT connection

## Service Accounts

Two service accounts are required for CI/CD:

**yc-sa-id** (deployment account) needs:
- functions.admin
- iam.serviceAccounts.user
- logging.editor
- vpc.user

**service-account** (function runtime account) needs:
- lockbox.payloadViewer
- iot.registries.writer
- iot.devices.writer
- kms.keys.encrypterDecrypter

## Logging

Uses custom JSON formatter (`YcLoggingFormatter`) for Yandex Cloud compatibility. All logging goes through `my_logger.logger` at INFO level.

## Key Implementation Details

**MQTT Publishing**: Commands are NOT published directly via MQTT client. Instead, they use Yandex IoT Core REST API (`iot-data.api.cloud.yandex.net`) with IAM token from function context.

**Serverless Lifecycle**: Each function invocation creates new MQTT connection, processes request, and cleans up before termination. No persistent connections.

**Timeout Configuration**: MQTT operations have tight timeouts (2s) since function execution time is limited and billed.

**Watering System Special Case**: The "water all" command (device_manager.py:239) sends `start_all` instead of on/off commands and doesn't verify state changes.
