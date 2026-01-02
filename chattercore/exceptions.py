"""
ChatterCore exceptions module

This module defines all custom exceptions used throughout the ChatterCore system.
"""

from typing import Optional, Any


class ChatterCoreException(Exception):
    """Base exception for all ChatterCore related errors."""
    
    def __init__(self, message: str, details: Optional[dict] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class ConnectionException(ChatterCoreException):
    """Raised when connection-related errors occur."""
    pass


class MessageException(ChatterCoreException):
    """Raised when message handling errors occur."""
    pass


class ServerException(ChatterCoreException):
    """Raised when server-specific errors occur."""
    pass


class ClientException(ChatterCoreException):
    """Raised when client-specific errors occur."""
    pass


class ValidationException(ChatterCoreException):
    """Raised when message validation fails."""
    pass


class TimeoutException(ChatterCoreException):
    """Raised when operations timeout."""
    pass


class AuthenticationException(ChatterCoreException):
    """Raised when authentication fails."""
    pass
