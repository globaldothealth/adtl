from __future__ import annotations

from pathlib import Path

import numpy.testing as npt
import pandas as pd
import pandera.pandas as pa
import pytest

from adtl.autoparser.util import (
    check_matches,
    load_data_dict,
    parse_llm_mapped_values,
    read_schema,
)


def test_read_schema():
    data = read_schema(Path("tests/test_autoparser/schemas/animals.schema.json"))
    assert isinstance(data, dict)
    npt.assert_array_equal(
        ["$schema", "$id", "title", "description", "required", "properties"],
        list(data.keys()),
    )

    with pytest.raises(ValueError, match="Unsupported file format: .csv"):
        read_schema(Path("tests/test_autoparser/sources/animals_dd_described.csv"))


@pytest.mark.parametrize(
    "s, expected",
    [
        ("oui=True, non=False, blah=None", {"oui": True, "non": False, "blah": ""}),
        ("vivant=alive, décédé=dead, =None", {"vivant": "alive", "décédé": "dead"}),
        ({2: True}, None),
        (" = , poisson=fish", {"poisson": "fish"}),
        (
            "=None, ecouvillon+croûte=[swab, crust], ecouvillon=[swab]",
            {"ecouvillon+croûte": ["swab", "crust"], "ecouvillon": ["swab"]},
        ),
        ("pos=Y, neg=N", {"pos": "Y", "neg": "N"}),
    ],
)
def test_parse_llm_mapped_values(s, expected):
    choices = parse_llm_mapped_values(s)
    assert choices == expected


def test_parse_llm_mapped_values_error():
    # dictionary printed without stringification
    with pytest.raises(ValueError, match="Invalid choices list"):
        parse_llm_mapped_values('{"oui":"True", "non":"False", "blah":"None"}')

    # different choice_delimeter_map
    with pytest.raises(ValueError, match="Invalid choices list"):
        parse_llm_mapped_values("oui:True, non:False, blah:None")


def test_load_data_dict_invalid():
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

    with pytest.raises(pa.errors.SchemaErrors):
        load_data_dict("tests/test_autoparser/sources/animals_dd.csv")


@pytest.mark.parametrize(
    "input, expected", [(("fish", ["fishes"]), "fishes"), (("fish", ["shark"]), None)]
)
def test_check_matches(input, expected):
    llm, source = input
    assert check_matches(llm, source) == expected


def test_check_matches_error():
    with pytest.raises(
        ValueError, match="check matches: source must be a list of strings, got 'fish'"
    ):
        check_matches("fish", "fish")
