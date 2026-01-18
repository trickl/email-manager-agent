# Email Manager Agent

Tooling and prototypes for **metadata-first Gmail analysis**.

This repository currently contains:

- A **Python CLI** (`email-manager`) that builds and queries a **local, header-only** Gmail index (SQLite + FTS).
- An **Email Intelligence** dashboard under `email-intelligence/` (FastAPI + React) that can ingest metadata into Postgres/Qdrant and run clustering + labeling using Ollama.

## Important notice: personal use, privacy & liability

This project was created for **personal use** and is provided **“as is”**, without warranty of any kind.

Because it connects to an email account and uses AI to analyze and act on messages, it can affect highly sensitive data. If you choose to use this code (or derive from it), please do so with **extreme caution** and only after you fully understand what it does.

By using this repository, you acknowledge and accept that:

- It may read, process, label, move, delete, or otherwise modify email data depending on configuration and future changes.
- AI systems can be unpredictable and may make mistakes.
- There is a real risk of **data loss** and/or **data exposure** if misconfigured, extended, or used incorrectly.

I do **not** accept liability for any damage, data loss, privacy impact, or data exposure resulting from use of this code.

This repository is shared with the open-source community to **inspire and help you build your own projects**—please be mindful, protect your privacy, and apply appropriate safeguards.

Note: both the CLI and the dashboard default to the **read-only** Gmail scope (`gmail.readonly`).

## Screenshot

Below is the (optional) Email Intelligence dashboard UI included under `email-intelligence/`.

![Email Intelligence dashboard screenshot](docs/screenshots/email-intelligence-dashboard.png)

## Features

### Header-only Gmail index CLI (`email-manager`)

- 📥 **Metadata-only download** (Gmail `format=metadata`) into a local SQLite database
- 🔎 **Fast local search** via SQLite FTS (subject search)
- 📈 **Quick stats** (total/unread counts, top senders)
- 🔐 Defaults to **`gmail.readonly`** and does **not** modify your mailbox

### Email Intelligence dashboard (`email-intelligence/`)

- 🧱 **Metadata-first ingestion** into Postgres + embeddings in Qdrant
- 🧠 **Clustering + labeling** using embeddings and Ollama (bodies are fetched only for representative samples during labeling)
- 🧭 **Explorable hierarchy** (category → subcategory → cluster → sender) with a tree + sunburst
- 🧪 **Job runner** endpoints with live progress streaming (SSE)
- 🗂️ **Message samples** for a selected node (metadata only)

### Not implemented (yet)

- Automatic unsubscribe actions
- Automatic label/move/delete operations against your Gmail mailbox
- The end-to-end automation methods in `EmailAgent` (`process_emails`, `categorize_email`) are placeholders

## Prerequisites

- Python 3.10 or higher
- Gmail API credentials (see [Gmail API Setup](#gmail-api-setup))

For the Email Intelligence dashboard (`email-intelligence/`):

- Docker + Docker Compose (for Postgres + Qdrant)
- Node.js (for the React UI)
- [Ollama](https://ollama.ai/) (recommended for embeddings + labeling)

## Installation

### Using pip

```bash
# Clone the repository
git clone https://github.com/trickl/email-manager-agent.git
cd email-manager-agent

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install the package with development dependencies
pip install -e ".[dev]"
```

### Using requirements.txt

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

## Configuration

### CLI configuration (repo root)

Create a `.env` file in the project root (or copy `.env.example` to `.env`):

```env
# Ollama Configuration
EMAIL_AGENT_OLLAMA_HOST=http://localhost:11434
EMAIL_AGENT_OLLAMA_MODEL=llama2

# Gmail Configuration
EMAIL_AGENT_GMAIL_CREDENTIALS_PATH=credentials.json
EMAIL_AGENT_GMAIL_TOKEN_PATH=token.json
EMAIL_AGENT_GMAIL_SCOPE=https://www.googleapis.com/auth/gmail.readonly

# Header index configuration
EMAIL_AGENT_INDEX_DB_PATH=email_index.sqlite3
EMAIL_AGENT_INDEX_BATCH_SIZE=200

# Application Configuration
EMAIL_AGENT_LOG_LEVEL=INFO
EMAIL_AGENT_DEBUG=false
EMAIL_AGENT_MAX_RETRIES=3

# Optional caching knobs (mostly used by future features)
EMAIL_AGENT_CACHE_ENABLED=true
EMAIL_AGENT_CACHE_TTL=3600

# Optional: list() pagination size (Gmail max page size is 500)
EMAIL_AGENT_GMAIL_MAX_RESULTS=100
```

### Email Intelligence backend configuration

Copy `email-intelligence/backend/.env.example` to `email-intelligence/backend/.env` and edit values as needed.
That backend uses variables prefixed with `EMAIL_INTEL_`.

## Gmail API Setup

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the Gmail API
4. Create OAuth 2.0 credentials (Desktop app)
5. Download the credentials and save as `credentials.json` in the project root

## Usage

```bash
# Run the CLI
email-manager

# Or run directly with Python
python -m email_manager_agent.cli
```

### Common CLI commands

```bash
# Build a local header-only index
email-manager index build --query "in:inbox" --limit 2000

# Search the local index (SQLite FTS query)
email-manager index search "invoice OR receipt"

# Show stats and top senders
email-manager index stats
```

### Running the Email Intelligence dashboard

See `email-intelligence/` for the full stack (FastAPI + Postgres + Qdrant + React UI).

## Development

### Setting up pre-commit hooks

```bash
pre-commit install
```

### Running tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=email_manager_agent

# Run only unit tests
pytest tests/unit

# Run only integration tests (requires Ollama and Gmail setup)
pytest tests/integration -m integration
```

### Code formatting and linting

```bash
# Format code with Black
black src/ tests/

# Lint with Ruff
ruff check src/ tests/

# Type check with MyPy
mypy src/
```

### Running all checks

```bash
# Run all pre-commit hooks
pre-commit run --all-files
```

## Project Structure

```
email-manager-agent/
├── src/email_manager_agent/    # Main package
│   ├── __init__.py
│   ├── cli.py                  # Command-line interface
│   ├── config.py               # Configuration management
│   ├── exceptions.py           # Custom exceptions
│   ├── gmail/                  # Gmail API client
│   ├── ollama/                 # Ollama LLM client
│   ├── agent/                  # Core agent logic
│   ├── models/                 # Data models (Pydantic)
│   └── utils/                  # Utility functions
├── tests/                      # Test suite
│   ├── unit/                   # Unit tests
│   └── integration/            # Integration tests
├── docs/                       # Documentation
├── scripts/                    # Utility scripts
├── .github/                    # GitHub configuration
│   └── copilot-instructions.md # GitHub Copilot instructions
├── pyproject.toml             # Project configuration
├── .pre-commit-config.yaml    # Pre-commit hooks
└── README.md                  # This file
```

## Architecture

The project follows these design principles:

- **Dependency Injection**: For testability and modularity
- **Repository Pattern**: Separates data access from business logic
- **Factory Pattern**: For creating configured clients
- **Strategy Pattern**: For different categorization strategies
- **Async/Await**: For efficient API calls and I/O operations

## Code Quality

This project maintains high code quality standards:

- **Type Hints**: All code is fully typed and checked with MyPy
- **Documentation**: Google-style docstrings for all public APIs
- **Testing**: >80% code coverage with pytest
- **Linting**: Ruff for fast, comprehensive linting
- **Formatting**: Black and Ruff for consistent code style
- **Security**: Bandit for security scanning

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests and linting (`pytest && ruff check . && black --check .`)
5. Commit your changes (`git commit -m 'feat: add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Ollama](https://ollama.ai/) - Local LLM inference
- [Gmail API](https://developers.google.com/gmail/api) - Email access
- [Pydantic](https://docs.pydantic.dev/) - Data validation
- [Structlog](https://www.structlog.org/) - Structured logging

## Support

For issues, questions, or contributions, please open an issue on [GitHub](https://github.com/trickl/email-manager-agent/issues)
