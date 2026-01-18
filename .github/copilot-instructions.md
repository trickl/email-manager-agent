# GitHub Copilot Instructions for Email Manager Agent

## Project Overview

## Safety & liability notice (README)

The `README.md` contains an **Important notice: personal use, privacy & liability** section near the top.

- **Do not remove it.**
- Do not weaken it, bury it, or move it to the bottom.
- If you update README wording elsewhere, ensure this notice remains clearly visible near the front of the file.

## Email Intelligence System planning (north-star)

For work under `email-intelligence/`, treat `email-intelligence/PLANNING.md` as the **target-state**
reference document (north star).

- It is **not** an implementation guide; it defines intended end-state behaviors and UX principles.
- Prefer changes that move the system toward the target state (metadata-first ingestion, explainable
    clustering-first workflows, UI that never hides uncertainty).
- If a proposed change conflicts with `email-intelligence/PLANNING.md`, update the change or explicitly
    document why the plan is being revised.

**Email Manager Agent** is an AI-powered automation tool designed to help users manage their email inboxes more efficiently. The agent uses:

- **Ollama**: A local LLM inference engine for AI-powered email categorization and content analysis
- **Gmail API**: Google's official API for reading, categorizing, and managing Gmail messages

### Core Functionality

The agent automatically:
1. Fetches and analyzes email messages from Gmail
2. Uses AI (via Ollama) to categorize emails by importance, topic, and intent
3. Identifies subscription emails and provides unsubscribe recommendations
4. Helps clean up and organize neglected inboxes
5. Learns from user preferences to improve categorization over time

## Architecture and Design Principles

### Project Structure

```
email-manager-agent/
├── src/email_manager_agent/     # Main package source code
│   ├── __init__.py
│   ├── cli.py                   # Command-line interface
│   ├── config.py                # Configuration management
│   ├── gmail/                   # Gmail API integration
│   ├── ollama/                  # Ollama LLM integration
│   ├── agent/                   # Core agent logic
│   ├── models/                  # Data models (Pydantic)
│   └── utils/                   # Utility functions
├── tests/                       # Test suite
│   ├── unit/                    # Unit tests
│   └── integration/             # Integration tests
├── docs/                        # Documentation
├── scripts/                     # Utility scripts
└── pyproject.toml              # Project configuration
```

### Design Patterns

- **Dependency Injection**: Use DI for testability and modularity
- **Repository Pattern**: Separate data access from business logic
- **Factory Pattern**: For creating configured clients (Gmail, Ollama)
- **Strategy Pattern**: For different categorization strategies
- **Async/Await**: Use async operations for API calls and I/O

## Python Best Practices

### Code Style and Formatting

1. **Line Length**: Maximum 100 characters
2. **Imports**: Organized using isort (part of Ruff)
   - Standard library imports first
   - Third-party imports second
   - Local application imports last
3. **String Quotes**: Use double quotes for strings
4. **Formatting**: Use Black and Ruff for consistent formatting

### Type Annotations

- **Always use type hints** for function signatures
- Use `typing` module types: `List`, `Dict`, `Optional`, `Union`, etc.
- For Python 3.10+, use modern syntax: `list[str]`, `dict[str, int]`
- Use Pydantic models for data validation and serialization

```python
from typing import Optional

def process_email(email_id: str, user_id: Optional[str] = None) -> dict[str, str]:
    """Process an email and return categorization results."""
    ...
```

### Documentation

- **Google-style docstrings** for all public functions, classes, and methods
- Include:
  - Brief description (one-line summary)
  - Detailed description (if needed)
  - Args: Parameter descriptions with types
  - Returns: Return value description with type
  - Raises: Any exceptions that may be raised
  - Example: Usage example for complex functions

```python
def categorize_email(content: str, model: str = "llama2") -> EmailCategory:
    """Categorize an email using Ollama LLM.

    This function sends the email content to a local Ollama instance
    and returns the categorization result based on the AI analysis.

    Args:
        content: The email content to categorize.
        model: The Ollama model to use. Defaults to "llama2".

    Returns:
        An EmailCategory object containing the categorization results.

    Raises:
        OllamaConnectionError: If unable to connect to Ollama.
        ValidationError: If the response format is invalid.

    Example:
        >>> category = categorize_email("Newsletter about Python updates")
        >>> print(category.type)
        'newsletter'
    """
    ...
```

### Error Handling

- **Use specific exceptions** instead of bare `Exception`
- Create custom exception classes in a dedicated module
- Always log errors with context using structlog
- Use try-except-else-finally appropriately
- Don't suppress exceptions silently

```python
from email_manager_agent.exceptions import GmailAPIError

try:
    emails = gmail_client.fetch_emails()
except GmailAPIError as e:
    logger.error("failed_to_fetch_emails", error=str(e), user_id=user_id)
    raise
```

### Testing

- **Write tests first** (TDD approach when possible)
- Aim for >80% code coverage
- Use pytest fixtures for setup/teardown
- Mock external API calls (Gmail, Ollama)
- Test both success and failure paths
- Use parametrize for testing multiple inputs

```python
import pytest
from unittest.mock import Mock, patch

@pytest.fixture
def mock_ollama_client():
    """Provide a mocked Ollama client."""
    return Mock()

@pytest.mark.parametrize("email_type,expected", [
    ("newsletter", "subscription"),
    ("personal", "important"),
])
def test_categorize_email(mock_ollama_client, email_type, expected):
    """Test email categorization with various email types."""
    ...
```

### Security Practices

- **Never commit credentials** or API keys
- Use environment variables for sensitive configuration
- Use `.env` files (never committed) for local development
- Validate and sanitize all user inputs
- Use OAuth2 for Gmail authentication
- Follow principle of least privilege for API scopes

### Logging

- Use **structlog** for structured logging
- Include relevant context in all log messages
- Use appropriate log levels:
  - DEBUG: Detailed diagnostic information
  - INFO: General informational messages
  - WARNING: Warning messages for potentially harmful situations
  - ERROR: Error messages for failures
  - CRITICAL: Critical failures requiring immediate attention

```python
import structlog

logger = structlog.get_logger()

logger.info(
    "email_categorized",
    email_id=email_id,
    category=category.type,
    confidence=category.confidence,
    processing_time_ms=elapsed_ms,
)
```

### Configuration Management

- Use Pydantic's BaseSettings for configuration
- Support both environment variables and .env files
- Validate configuration on startup
- Provide sensible defaults where appropriate

```python
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """Application settings."""

    ollama_host: str = "http://localhost:11434"
    ollama_model: str = "llama2"
    gmail_credentials_path: str = "credentials.json"
    log_level: str = "INFO"

    class Config:
        env_prefix = "EMAIL_AGENT_"
        env_file = ".env"
```

## AI/LLM Guidelines

### Ollama Integration

- Use async operations for LLM inference
- Implement proper timeout handling
- Handle model loading failures gracefully
- Cache responses when appropriate
- Implement retry logic with exponential backoff

### Prompt Engineering

- Use clear, specific prompts
- Include examples in prompts (few-shot learning)
- Structure prompts for consistent outputs
- Request structured JSON responses
- Include safety instructions to prevent misuse

```python
CATEGORIZATION_PROMPT = """
You are an email categorization assistant. Analyze the following email and categorize it.

Email Subject: {subject}
Email Body: {body}

Respond with JSON in the following format:
{
    "category": "personal|work|newsletter|promotional|spam",
    "importance": "high|medium|low",
    "confidence": 0.0-1.0,
    "reasoning": "brief explanation"
}
"""
```

## Gmail API Guidelines

### Authentication

- Use OAuth2 with appropriate scopes
- Store tokens securely
- Implement token refresh logic
- Handle authentication errors gracefully

### Rate Limiting

- Respect Gmail API rate limits
- Implement exponential backoff for retries
- Batch operations when possible
- Cache frequently accessed data

### Privacy

- Only request necessary Gmail scopes
- Minimize data retention
- Provide clear privacy disclosures
- Allow users to revoke access

## Development Workflow

1. **Create feature branch** from main
2. **Write tests** before implementation
3. **Implement feature** with proper type hints and documentation
4. **Run linters**: `ruff check . && black --check .`
5. **Run type checker**: `mypy src/`
6. **Run tests**: `pytest`
7. **Check coverage**: Coverage should not decrease
8. **Commit with conventional commits**: feat:, fix:, docs:, etc.
9. **Create pull request** with clear description

## Code Quality Standards

### Pre-commit Checks

Before committing, the following checks run automatically:
- Code formatting (Black, Ruff)
- Linting (Ruff)
- Type checking (MyPy)
- Security scanning (Bandit)
- Import sorting
- Trailing whitespace removal
- YAML/JSON validation

### Continuous Integration

All PRs must pass:
- Unit tests (with >80% coverage)
- Integration tests
- Linting and formatting checks
- Type checking
- Security scans

## Common Patterns

### Async API Calls

```python
import asyncio
from typing import List

async def fetch_recent_emails(limit: int = 50) -> List[EmailMessage]:
    """Fetch recent emails asynchronously."""
    async with GmailClient() as client:
        messages = await client.list_messages(max_results=limit)
        return messages
```

### Error Handling

```python
from email_manager_agent.exceptions import AgentError

def process_with_retry(func, max_retries: int = 3):
    """Execute function with retry logic."""
    for attempt in range(max_retries):
        try:
            return func()
        except AgentError as e:
            if attempt == max_retries - 1:
                raise
            logger.warning("retry_attempt", attempt=attempt + 1, error=str(e))
            time.sleep(2 ** attempt)  # Exponential backoff
```

### Configuration Loading

```python
from functools import lru_cache

@lru_cache()
def get_settings() -> Settings:
    """Get cached application settings."""
    return Settings()
```

## Dependencies

### Core Dependencies

- `google-api-python-client`: Gmail API integration
- `google-auth`: Google authentication
- `ollama`: Ollama Python client
- `pydantic`: Data validation and settings
- `structlog`: Structured logging
- `python-dotenv`: Environment variable management

### Development Dependencies

- `pytest`: Testing framework
- `pytest-cov`: Coverage reporting
- `pytest-asyncio`: Async test support
- `ruff`: Fast Python linter
- `black`: Code formatter
- `mypy`: Static type checker
- `pre-commit`: Git hook framework

## Resources

- [Python Style Guide (PEP 8)](https://pep8.org/)
- [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)
- [Gmail API Documentation](https://developers.google.com/gmail/api)
- [Ollama Python Documentation](https://github.com/ollama/ollama-python)
- [Pydantic Documentation](https://docs.pydantic.dev/)
- [Structlog Documentation](https://www.structlog.org/)

## Important Notes

- **Always run tests** before committing
- **Keep dependencies minimal** - only add what's necessary
- **Document breaking changes** in commit messages
- **Update README** when adding new features
- **Security first** - never expose credentials or sensitive data
- **Performance matters** - profile and optimize hot paths
- **User privacy** is paramount - handle email data with care
