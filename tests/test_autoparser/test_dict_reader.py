# tests the `DictReader` class
from __future__ import annotations

import os

import pandas as pd
import pytest
from pandera.errors import SchemaError
from pytest_unordered import unordered

from adtl.autoparser import setup_config
from adtl.autoparser.dict_reader import DictReader, main

SOURCES = "tests/test_autoparser/sources/"
SCHEMAS = "tests/test_autoparser/schemas/"


@pytest.fixture
def config():
    setup_config(
        {
            "name": "Test Config",
            "language": "en",
            "choice_delimiter": "|",
            "choice_delimiter_map": ",",
            "max_common_count": 8,
            "column_mappings": {
                "source_field": "Variable / Field Name",
                "source_description": "Field Label",
                "source_type": "Field Type",
                "common_values": None,
                "choices": "Choices, Calculations, OR Slider Labels",
            },
            "schemas": {"animals": "animals.schema.json"},
        }
    )


@pytest.fixture
def reader(config):
    return DictReader(data_dict=SOURCES + "sample_data_dict.csv")


@pytest.fixture
def reader_dedupe(reader):
    # Remove duplicate row for testing
    reader.data_dict.drop(8, inplace=True)
    reader.data_dict.reset_index(drop=True, inplace=True)
    return reader


def test_valid_data_dict(reader_dedupe):
    # Check if the data dictionary is read correctly
    assert isinstance(reader_dedupe.data_dict, pd.DataFrame)
    assert not reader_dedupe.data_dict.empty

    converted_dict = reader_dedupe.validate_dictionary(save=False)

    assert list(converted_dict.columns) == [
        "source_field",
        "source_type",
        "source_description",
        "choices",
    ]

    pd.testing.assert_series_equal(
        converted_dict["choices"],
        pd.Series(
            [
                None,
                None,
                None,
                None,
                None,
                None,
                {"1": "kg", "2": "lbs"},
                {"1": "Afghanistan", "2": "Albania", "3": "Algeria", "4": "Andorra"},
                {"1": "°C", "2": "°F"},
            ]
        ),
        check_names=False,
    )


def test_invalid_data_dict_duplicates(reader):
    with pytest.raises(
        SchemaError, match="Data dictionary validation failed with 2 error"
    ):
        reader.validate_dictionary()


def test_process_dict():
    setup_config(
        {
            "name": "test_autoparser",
            "language": "en",
            "max_common_count": 8,
            "schemas": {"animals": "tests/test_autoparser/schemas/animals.schema.json"},
        }
    )
    reader = DictReader(data_dict=SOURCES + "animals_dd_described.csv")

    dd = pd.DataFrame(
        {
            "source_field": ["field_1", "field_2", "field_3"],
            "source_description": ["name", "alive", "species"],
            "source_type": ["string", "str", "choice"],
            "common_values": [None, "Oui, NON, OUI, oui", "cat, dog, fish"],
        }
    )
    processed_common_vals = reader._process_dict(dd)

    assert processed_common_vals["common_values"].tolist() == [
        None,
        unordered(["oui", "non"]),
        unordered(["cat", "fish", "dog"]),
    ]


def test_save_formatted_dict(tmp_path, reader_dedupe):
    file_name = tmp_path / "saved_dict.parquet"

    df = reader_dedupe.validate_dictionary(save=False)
    reader_dedupe.save_formatted_dictionary(name=file_name)

    loaded_file = pd.read_parquet(file_name, engine="fastparquet")
    assert loaded_file.equals(df)


def test_main_cli(tmp_path, reader_dedupe):
    # create a save, deduplicated version of the data dictionary
    reader_dedupe.data_dict.to_csv(
        tmp_path / "sample_data_dict_dedupe.csv", index=False
    )

    ARGV = [
        str(tmp_path / "sample_data_dict_dedupe.csv"),
        "-c",
        os.path.join(SOURCES, "test_config_provided_dict.toml"),
        "-o",
        str(tmp_path / "test_dd_validation"),
    ]

    main(ARGV)

    assert (tmp_path / "test_dd_validation.parquet").exists()
