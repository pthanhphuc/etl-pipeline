"""Allow ``python -m etl`` to invoke the CLI."""

from .cli import main


raise SystemExit(main())
