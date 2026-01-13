from __future__ import annotations


class InfraError(Exception):
    """Base class for infra layer errors."""


class NotFoundError(InfraError):
    """Raised when a requested entity cannot be found."""


class ValidationError(InfraError):
    """Raised when a config, contract, or input fails validation."""


class RetryableError(InfraError):
    """Raised when an operation may succeed if retried."""


class ConflictError(InfraError):
    """Raised when an operation conflicts with existing state."""


class NotConfiguredError(InfraError):
    """Raised when a requested adapter is declared but not wired for the current runtime."""
