import pytest

import adtl.autoparser as autoparser

CONFIG_PATH = "tests/test_autoparser/test_config.toml"
SOURCES = "tests/test_autoparser/sources/"
SCHEMAS = "tests/test_autoparser/schemas/"
# ARGV = [
#     str(TEST_PARSERS_PATH / "epoch.json"),
#     str(TEST_SOURCES_PATH / "epoch.csv"),
#     "-o",
#     "output",
#     "--encoding",
#     "utf-8",
# ]


def test_main_unrecognised():
    with pytest.raises(SystemExit):
        autoparser.main(["adtl-autoparser", "fish"])
