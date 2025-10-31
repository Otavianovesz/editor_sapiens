"""
Custom exceptions module for Editor Sapiens.
Provides a clear exception hierarchy for better error handling.
"""

class SapiensError(Exception):
    """Base class for all Editor Sapiens exceptions."""
    pass

class ResourceError(SapiensError):
    """Errors related to resources (files, memory, GPU)."""
    pass

class ProcessingError(SapiensError):
    """Errors during data processing."""
    pass

class ValidationError(SapiensError):
    """Errors in data or configuration validation."""
    pass

class StateError(SapiensError):
    """Errors related to invalid states."""
    pass

class InterruptedError(SapiensError):
    """Error when an operation is interrupted by the user."""
    pass
