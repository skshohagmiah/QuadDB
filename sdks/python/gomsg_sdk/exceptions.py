"""
fluxdl SDK Exceptions
"""


class fluxdlError(Exception):
    """Base exception for all fluxdl SDK errors."""
    
    def __init__(self, message: str, code: str = None):
        super().__init__(message)
        self.message = message
        self.code = code


class ConnectionError(fluxdlError):
    """Raised when connection to fluxdl server fails."""
    
    def __init__(self, message: str):
        super().__init__(message, "CONNECTION_ERROR")


class TimeoutError(fluxdlError):
    """Raised when request times out."""
    
    def __init__(self, message: str):
        super().__init__(message, "TIMEOUT_ERROR")


class AuthenticationError(fluxdlError):
    """Raised when authentication fails."""
    
    def __init__(self, message: str):
        super().__init__(message, "AUTH_ERROR")


class KeyNotFoundError(fluxdlError):
    """Raised when a key is not found in KV store."""
    
    def __init__(self, key: str):
        super().__init__(f"Key '{key}' not found", "KEY_NOT_FOUND")
        self.key = key


class QueueEmptyError(fluxdlError):
    """Raised when trying to pop from an empty queue."""
    
    def __init__(self, queue: str):
        super().__init__(f"Queue '{queue}' is empty", "QUEUE_EMPTY")
        self.queue = queue


class StreamNotFoundError(fluxdlError):
    """Raised when a stream is not found."""
    
    def __init__(self, stream: str):
        super().__init__(f"Stream '{stream}' not found", "STREAM_NOT_FOUND")
        self.stream = stream
