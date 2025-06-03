from __future__ import annotations

from pathlib import Path

import numpy.testing as npt
import pandas as pd
import pandera.pandas as pa
import pytest

from adtl.autoparser.language_models.gemini import GeminiLanguageModel
from adtl.autoparser.util import (
    check_matches,
    load_data_dict,
    parse_llm_mapped_values,
    read_config_schema,
    setup_llm,
)

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
            "max_common_count",
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


def test_setup_llm_no_key():
    with pytest.raises(ValueError, match="API key required to set up an LLM"):
        setup_llm(None, provider="openai")


def test_setup_llm_bad_provider():
    with pytest.raises(ValueError, match="Unsupported LLM provider: fish"):
        setup_llm("abcd", provider="fish")


def test_setup_llm_provide_model():
    model = setup_llm("abcd", provider="gemini", model="gemini-2.0-flash")
    assert model.model == "gemini-2.0-flash"


def test_setup_llm_provide_model_no_provider():
    model = setup_llm("abcd", model="gemini-2.0-flash")
    assert isinstance(model, GeminiLanguageModel)
    assert model.model == "gemini-2.0-flash"


def test_setup_llm_no_provider_no_model():
    with pytest.raises(
        ValueError, match="Either a provider, a model or both must be provided"
    ):
        setup_llm("1234")


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
