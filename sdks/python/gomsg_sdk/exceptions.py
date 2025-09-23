"""
GoMsg SDK Exceptions
"""


class GoMsgError(Exception):
    """Base exception for all GoMsg SDK errors."""
    
    def __init__(self, message: str, code: str = None):
        super().__init__(message)
        self.message = message
        self.code = code


class ConnectionError(GoMsgError):
    """Raised when connection to GoMsg server fails."""
    
    def __init__(self, message: str):
        super().__init__(message, "CONNECTION_ERROR")


class TimeoutError(GoMsgError):
    """Raised when request times out."""
    
    def __init__(self, message: str):
        super().__init__(message, "TIMEOUT_ERROR")


class AuthenticationError(GoMsgError):
    """Raised when authentication fails."""
    
    def __init__(self, message: str):
        super().__init__(message, "AUTH_ERROR")


class KeyNotFoundError(GoMsgError):
    """Raised when a key is not found in KV store."""
    
    def __init__(self, key: str):
        super().__init__(f"Key '{key}' not found", "KEY_NOT_FOUND")
        self.key = key


class QueueEmptyError(GoMsgError):
    """Raised when trying to pop from an empty queue."""
    
    def __init__(self, queue: str):
        super().__init__(f"Queue '{queue}' is empty", "QUEUE_EMPTY")
        self.queue = queue


class StreamNotFoundError(GoMsgError):
    """Raised when a stream is not found."""
    
    def __init__(self, stream: str):
        super().__init__(f"Stream '{stream}' not found", "STREAM_NOT_FOUND")
        self.stream = stream
