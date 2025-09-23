"""
fluxdl Python Client

Main client class for connecting to fluxdl server.
"""

import asyncio
from typing import Optional, Dict, Any
import grpc
from .kv import KVClient
from .queue import QueueClient
from .stream import StreamClient
from .exceptions import ConnectionError, TimeoutError


class fluxdlClient:
    """
    Main fluxdl client for connecting to fluxdl Docker containers.
    
    Example:
        client = fluxdlClient(address="localhost:9000")
        await client.connect()
        
        # Key-Value operations
        await client.kv.set("key", "value")
        value = await client.kv.get("key")
        
        # Queue operations  
        await client.queue.push("tasks", "message")
        message = await client.queue.pop("tasks")
        
        # Stream operations
        await client.stream.publish("events", "data")
    """
    
    def __init__(
        self,
        address: str = "localhost:9000",
        timeout: float = 30.0,
        credentials: Optional[grpc.ChannelCredentials] = None,
        options: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize fluxdl client.
        
        Args:
            address: Server address (default: "localhost:9000")
            timeout: Request timeout in seconds (default: 30.0)
            credentials: gRPC credentials (default: None for insecure)
            options: Additional gRPC channel options
        """
        self.address = address
        self.timeout = timeout
        self.credentials = credentials
        self.options = options or {}
        
        self._channel: Optional[grpc.aio.Channel] = None
        self._kv: Optional[KVClient] = None
        self._queue: Optional[QueueClient] = None
        self._stream: Optional[StreamClient] = None
        self._connected = False

    async def connect(self) -> None:
        """Connect to fluxdl server."""
        try:
            # Create gRPC channel
            if self.credentials:
                self._channel = grpc.aio.secure_channel(
                    self.address, 
                    self.credentials,
                    options=list(self.options.items())
                )
            else:
                self._channel = grpc.aio.insecure_channel(
                    self.address,
                    options=list(self.options.items())
                )
            
            # Initialize clients
            self._kv = KVClient(self._channel, self.timeout)
            self._queue = QueueClient(self._channel, self.timeout)
            self._stream = StreamClient(self._channel, self.timeout)
            
            # Test connection
            await self.ping()
            self._connected = True
            
            print(f"✅ Connected to fluxdl at {self.address}")
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to fluxdl: {e}")

    async def disconnect(self) -> None:
        """Disconnect from fluxdl server."""
        if self._channel:
            await self._channel.close()
            self._channel = None
            self._connected = False
            print("Disconnected from fluxdl")

    async def ping(self) -> bool:
        """Test connection to server."""
        try:
            # Use a simple KV operation to test connectivity
            await self.kv.set("ping", "pong")
            await self.kv.delete("ping")
            return True
        except Exception:
            return False

    @property
    def kv(self) -> KVClient:
        """Get Key-Value client."""
        if not self._connected or not self._kv:
            raise ConnectionError("Not connected. Call connect() first.")
        return self._kv

    @property
    def queue(self) -> QueueClient:
        """Get Queue client."""
        if not self._connected or not self._queue:
            raise ConnectionError("Not connected. Call connect() first.")
        return self._queue

    @property
    def stream(self) -> StreamClient:
        """Get Stream client."""
        if not self._connected or not self._stream:
            raise ConnectionError("Not connected. Call connect() first.")
        return self._stream

    @property
    def connected(self) -> bool:
        """Check if client is connected."""
        return self._connected

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()

    @classmethod
    async def create(
        cls,
        address: str = "localhost:9000",
        timeout: float = 30.0,
        **kwargs
    ) -> "fluxdlClient":
        """
        Create and connect to fluxdl in one step.
        
        Args:
            address: Server address
            timeout: Request timeout
            **kwargs: Additional client options
            
        Returns:
            Connected fluxdlClient instance
        """
        client = cls(address=address, timeout=timeout, **kwargs)
        await client.connect()
        return client
