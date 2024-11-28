from pathlib import Path

import numpy.testing as npt
import pandas as pd
import pytest

from adtl.autoparser.util import load_data_dict, parse_choices, read_config_schema

CONFIG = read_config_schema(Path("tests/test_autoparser/test_config.toml"))


def test_read_config_schema():
    data = read_config_schema(Path("tests/test_autoparser/test_config.toml"))
    assert isinstance(data, dict)
    npt.assert_array_equal(
        [
            "name",
            "description",
            "choice_delimiter",
            "choice_delimiter_map",
            "num_refs",
            "num_choices",
            "schemas",
            "column_mappings",
        ],
        list(data.keys()),
    )

    data = read_config_schema(Path("tests/test_autoparser/schemas/animals.schema.json"))
    assert isinstance(data, dict)
    npt.assert_array_equal(
        ["$schema", "$id", "title", "description", "required", "properties"],
        list(data.keys()),
    )

    with pytest.raises(ValueError, match="Unsupported file format: .csv"):
        read_config_schema(
            Path("tests/test_autoparser/sources/animals_dd_described.csv")
        )


@pytest.mark.parametrize(
    "s, expected",
    [
        ("oui=True, non=False, blah=None", {"oui": True, "non": False, "blah": ""}),
        ("vivant=alive, décédé=dead, " "=None", {"vivant": "alive", "décédé": "dead"}),
        ({2: True}, None),
        ("" " = " ", poisson=fish", {"poisson": "fish"}),
    ],
)
def test_parse_choices(s, expected):
    choices = parse_choices(CONFIG, s)
    assert choices == expected


def test_parse_choices_error():
    # dictionary printed without stringification
    with pytest.raises(ValueError, match="Invalid choices list"):
        parse_choices(CONFIG, '{"oui":"True", "non":"False", "blah":"None"}')

    # different choice_delimeter_map
    with pytest.raises(ValueError, match="Invalid choices list"):
        parse_choices(CONFIG, "oui:True, non:False, blah:None")


def test_load_data_dict():
    dd_original = pd.read_csv("tests/test_autoparser/sources/animals_dd.csv")

    npt.assert_array_equal(
        list(dd_original.columns),
        [
            "Field Name",
            "Description",
            "Field Type",
            "Common Values",
        ],
    )

    data = load_data_dict(CONFIG, "tests/test_autoparser/sources/animals_dd.csv")
    npt.assert_array_equal(
        data.columns,
        ["source_field", "source_description", "source_type", "common_values"],
    )

    with pytest.raises(ValueError, match="Unsupported format"):
        load_data_dict(CONFIG, "tests/test_autoparser/sources/animals.txt")
