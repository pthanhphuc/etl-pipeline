"""Command-line interface for local ETL execution."""

import argparse
import json
import logging

from .errors import EtlError
from .logging_config import configure_logging

LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the local pipeline command parser."""

    parser = argparse.ArgumentParser(
        prog="etl",
        description="Metadata-driven ETL pipeline",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="console logging level (default: INFO)",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")

    init = subparsers.add_parser("init-db", help="initialize PostgreSQL tables")
    init.set_defaults(handler=_init_db)
    _source_command(subparsers.add_parser("transform", help="transform a source partition"), _transform)
    _source_command(subparsers.add_parser("load", help="load trusted output into PostgreSQL"), _load)
    _source_command(subparsers.add_parser("run", help="transform then load a source partition"), _run)
    _source_command(subparsers.add_parser("report", help="show a partition summary"), _report)
    promote = subparsers.add_parser("promote-warning", aliases=["promote"], help="promote corrected warning rows")
    promote.add_argument("--source-prefix", required=True)
    promote.add_argument("--corrected", required=True, help="corrected Parquet file")
    promote.add_argument("--run-id")
    promote.set_defaults(handler=_promote)

    return parser


def _source_command(parser: argparse.ArgumentParser, handler: object) -> None:
    parser.add_argument("--source-prefix", required=True, help="configured raw_data dataset/date prefix")
    parser.add_argument("--run-id", help="isolated transform run ID")
    parser.set_defaults(handler=handler)


def _init_db(args: argparse.Namespace) -> None:
    from .pipeline import init_database
    init_database()


def _transform(args: argparse.Namespace) -> None:
    from .pipeline import transform_source
    result = transform_source(args.source_prefix, run_id=args.run_id)
    print(json.dumps({"run_id": result.run_id, "trusted": str(result.output.trusted_path), "warning": str(result.output.warning_path)}, indent=2))


def _load(args: argparse.Namespace) -> None:
    from .pipeline import load_source
    result = load_source(args.source_prefix, run_id=args.run_id)
    print(json.dumps({"inserts": len(result.inserts), "updates": len(result.updates), "unchanged": len(result.unchanged), "duplicates": len(result.duplicates)}))


def _run(args: argparse.Namespace) -> None:
    from .pipeline import load_source, transform_source
    result = transform_source(args.source_prefix, run_id=args.run_id)
    decision = load_source(args.source_prefix, run_id=result.run_id)
    print(json.dumps({"run_id": result.run_id, "inserted": len(decision.inserts), "updated": len(decision.updates)}, indent=2))


def _report(args: argparse.Namespace) -> None:
    from .pipeline import read_report
    print(json.dumps(read_report(args.source_prefix, run_id=args.run_id), indent=2, sort_keys=True))


def _promote(args: argparse.Namespace) -> None:
    from .pipeline import promote_warning
    print(promote_warning(args.source_prefix, args.corrected, run_id=args.run_id))


def main(argv: list[str] | None = None) -> int:
    """Run the CLI and return a process exit code."""

    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)

    if not hasattr(args, "handler"):
        parser.print_help()
        return 0

    try:
        args.handler(args)
    except EtlError as exc:
        LOGGER.error("%s", exc)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
