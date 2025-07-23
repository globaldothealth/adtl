import pytest

import adtl.autoparser as autoparser

CONFIG_PATH = "tests/test_autoparser/test_config.toml"
SOURCES = "tests/test_autoparser/sources/"
SCHEMAS = "tests/test_autoparser/schemas/"


def test_main_unrecognised(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["adtl-autoparser", "fish"])

    with pytest.raises(SystemExit, match="2"):
        autoparser.main()

    captured = capsys.readouterr()
    assert "argument subcommand: invalid choice: 'fish'" in captured.err


def test_main_help(monkeypatch):
    monkeypatch.setattr("sys.argv", ["adtl-autoparser"])

    with pytest.raises(SystemExit, match="1"):
        autoparser.main()


def test_main_passes_args_to_subfunction(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["adtl-autoparser", "create-mapping", "dict"])

    with pytest.raises(SystemExit, match="2"):
        autoparser.main()

    captured = capsys.readouterr()
    assert (
        "autoparser create-mapping: error: the following arguments are required: table_name"
        in captured.err
    )
