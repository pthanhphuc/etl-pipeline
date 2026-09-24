"""Application-specific errors for the ETL command line interface."""


class EtlError(Exception):
    """Base class for expected ETL application errors."""


class ConfigurationError(EtlError):
    """Raised when application configuration is invalid."""
