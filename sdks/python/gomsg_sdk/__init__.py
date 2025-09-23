"""
fluxdl Python SDK

A Python client library for fluxdl - distributed data platform that replaces Redis + RabbitMQ + Kafka.
"""

from .client import fluxdlClient
from .kv import KVClient
from .queue import QueueClient
from .stream import StreamClient
from .exceptions import (
    fluxdlError,
    ConnectionError,
    TimeoutError,
    AuthenticationError,
)

__version__ = "1.0.0"
__author__ = "Shohag"
__email__ = "shohag2100@example.com"

__all__ = [
    "fluxdlClient",
    "KVClient", 
    "QueueClient",
    "StreamClient",
    "fluxdlError",
    "ConnectionError",
    "TimeoutError",
    "AuthenticationError",
]
