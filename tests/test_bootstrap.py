from etl.cli import build_parser


def test_cli_parser_has_phase_zero_commands() -> None:
    parser = build_parser()
    args = parser.parse_args(["--log-level", "DEBUG", "transform"])

    assert args.command == "transform"
    assert args.log_level == "DEBUG"
