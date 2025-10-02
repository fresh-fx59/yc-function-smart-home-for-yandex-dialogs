from index import RequestType
from my_logger import logger

DEVICE_UNREACHABLE_CODE = "DEVICE_UNREACHABLE"
ERROR_STATUS = "ERROR"


def get_error_response(message: str) -> dict:
    message_to_return = {
        "status": ERROR_STATUS,
        "error_code": DEVICE_UNREACHABLE_CODE,
        "error_message": message
    }
    logger.error(message)
    return message_to_return


def create_error_response(
        request_data,
        request_type: RequestType,
        error_code=DEVICE_UNREACHABLE_CODE,
        error_message="Device unreachable"
) -> dict:
    """
    Create an error response using device IDs from the request.

    Args:
        request_data (dict): The incoming request dictionary
        request_type (RequestType): The type of incoming request
        error_code (str): The error code to use (default: "DEVICE_UNREACHABLE")
        error_message (str, optional): Optional human-readable error message

    Returns:
        dict: The formatted error response
    """
    # Extract request_id from headers
    request_id = request_data.get("headers", {}).get("request_id", "")

    # Extract device IDs from the request payload
    devices = request_data.get("payload", {}).get("devices", [])

    # Build error response devices list
    error_devices = []
    for device in devices:
        if request_type == RequestType.ACTION.value:
            device_error = {
                "id": device.get("id"),
                "action_result": {
                    "status": ERROR_STATUS,
                    "error_code": error_code,
                    "error_message": error_message
                }
            }
        elif request_type == RequestType.QUERY.value:
            device_error = {
                "id": device.get("id"),
                "status": ERROR_STATUS,
                "error_code": error_code,
                "error_message": error_message
            }
        else:
            logger.error(f"Unknown request type = {request_type}")
            device_error = {
                "id": device.get("id"),
                "status": ERROR_STATUS,
                "error_code": error_code,
                "error_message": error_message
            }

        error_devices.append(device_error)

    # Construct the response
    response = {
        "request_id": request_id,
        "payload": {
            "devices": error_devices
        }
    }

    return response
