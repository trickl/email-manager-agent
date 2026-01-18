"""Command-line interface for Email Manager Agent.

This module provides the main entry point for the CLI application.
"""

import sys
from typing import Optional

import structlog

from email_manager_agent.config import get_settings

logger = structlog.get_logger()


def main(args: Optional[list[str]] = None) -> int:
    """Main entry point for the Email Manager Agent CLI.

    Args:
        args: Command-line arguments. If None, uses sys.argv.

    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    if args is None:
        args = sys.argv[1:]

    settings = get_settings()

    # Configure logging
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(structlog.stdlib, settings.log_level)
        ),
    )

    logger.info("email_manager_agent_started", version="0.1.0", debug=settings.debug)

    # TODO: Implement CLI argument parsing and command execution
    logger.warning("cli_not_implemented", message="CLI functionality coming soon")

    return 0


if __name__ == "__main__":
    sys.exit(main())
