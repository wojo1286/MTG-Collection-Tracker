from mtg_tracker.cli import main


def test_cli_stub_command_runs() -> None:
    assert main(["--config", "config.yaml", "seed"]) == 0
