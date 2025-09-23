"""
GoMsg Python SDK

A Python client library for GoMsg - distributed data platform that replaces Redis + RabbitMQ + Kafka.
"""

from .client import GoMsgClient
from .kv import KVClient
from .queue import QueueClient
from .stream import StreamClient
from .exceptions import (
    GoMsgError,
    ConnectionError,
    TimeoutError,
    AuthenticationError,
)

__version__ = "1.0.0"
__author__ = "Shohag"
__email__ = "shohag2100@example.com"

__all__ = [
    "GoMsgClient",
    "KVClient", 
    "QueueClient",
    "StreamClient",
    "GoMsgError",
    "ConnectionError",
    "TimeoutError",
    "AuthenticationError",
]
