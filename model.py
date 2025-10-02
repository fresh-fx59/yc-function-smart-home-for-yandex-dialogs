from enum import Enum


class RequestType(Enum):
    """Yandex Smart Home request types"""
    UNLINK = "unlink"
    DISCOVERY = "discovery"
    QUERY = "query"
    ACTION = "action"
