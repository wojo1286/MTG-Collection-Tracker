from pathlib import Path

from mtg_tracker.config import load_config


def test_load_config_reads_yaml_compatible_json(tmp_path: Path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text('{"state_backend": {"kind": "local_path"}}', encoding="utf-8")

    config = load_config(config_file)

    assert config.backend_kind == "local_path"
