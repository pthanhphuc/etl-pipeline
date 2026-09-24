"""Small, reusable logging setup for command-line entry points."""

import logging


def configure_logging(level: str = "INFO") -> None:
    """Configure predictable console logging once for the application."""

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
