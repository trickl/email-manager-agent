"""Custom exceptions for Email Manager Agent."""


class EmailManagerError(Exception):
    """Base exception for all Email Manager Agent errors."""


class GmailAPIError(EmailManagerError):
    """Exception raised for Gmail API related errors."""


class OllamaConnectionError(EmailManagerError):
    """Exception raised when unable to connect to Ollama."""


class OllamaInferenceError(EmailManagerError):
    """Exception raised when Ollama inference fails."""


class ConfigurationError(EmailManagerError):
    """Exception raised for configuration related errors."""


class AuthenticationError(EmailManagerError):
    """Exception raised for authentication failures."""


class ValidationError(EmailManagerError):
    """Exception raised for data validation errors."""
