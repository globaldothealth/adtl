# tests the `DictWriter` class
from __future__ import annotations

import pandas as pd
import pytest
from pandera.errors import SchemaError
from testing_data_animals import TestLLM

import adtl.autoparser as autoparser
from adtl.autoparser import setup_config
from adtl.autoparser.dict_writer import DictWriter, main
from adtl.autoparser.language_models.openai import OpenAILanguageModel

CONFIG_PATH = "tests/test_autoparser/test_config.toml"
SOURCES = "tests/test_autoparser/sources/"
SCHEMAS = "tests/test_autoparser/schemas/"


@pytest.fixture
def config():
    """Fixture to load the configuration for the autoparser."""
    setup_config(
        {
            "name": "test_autoparser",
            "choice_delimiter": ",",
            "max_common_count": 8,
            "schemas": {"animals": "tests/test_autoparser/schemas/animals.schema.json"},
        }
    )


@pytest.fixture
def writer(config):
    return DictWriter()


def test_unsupported_data_format_txt(writer):
    with pytest.raises(ValueError, match="Unsupported format"):
        writer.create_dict(SOURCES + "animals.txt")


def test_data_not_df_or_path(writer):
    with pytest.raises(ValueError, match="Data must be a path"):
        writer.create_dict(None)


def test_dictionary_creation_no_descrip(writer):
    df = writer.create_dict(SOURCES + "animal_data.csv")

    df_desired = pd.read_csv(SOURCES + "animals_dd.csv")

    pd.testing.assert_frame_equal(df, df_desired)


def test_create_dict_no_descrip(config):
    df = autoparser.create_dict(SOURCES + "animal_data.csv")

    df_desired = pd.read_csv(SOURCES + "animals_dd.csv")

    pd.testing.assert_frame_equal(df, df_desired)


@pytest.mark.filterwarnings("ignore:Small Dataset")
def test_dictionary_creation_no_descrip_excel(writer):
    # check no errors excel
    writer.create_dict(SOURCES + "animal_data.xlsx")


def test_dictionary_creation_no_descrip_dataframe(writer):
    # check no errors dataframe
    df = pd.read_csv(SOURCES + "animal_data.csv")
    writer.create_dict(df)


@pytest.mark.filterwarnings("ignore:Small Dataset")
def test_dictionary_creation_with_list(writer):
    df = writer.create_dict(SOURCES + "IB_sample_data.csv")

    df_desired = pd.read_csv(SOURCES + "IB_sample_dd.csv")

    pd.testing.assert_frame_equal(df, df_desired)


def test_dictionary_description(writer):
    writer.model = TestLLM()

    # check descriptions aren't generated without a dictionary
    with pytest.raises(ValueError, match="No data dictionary found"):
        writer.generate_descriptions("fr")

    df = writer.generate_descriptions("fr", SOURCES + "animals_dd.csv")

    df_desired = pd.read_csv(SOURCES + "animals_dd_described.csv")

    pd.testing.assert_frame_equal(df, df_desired)


def test_missing_key_error():
    with pytest.raises(ValueError, match="API key required"):
        DictWriter().generate_descriptions("fr", SOURCES + "animals_dd.csv")


def test_wrong_llm_error():
    with pytest.raises(ValueError, match="Unsupported LLM provider: fish"):
        DictWriter().generate_descriptions(
            "fr", SOURCES + "animals_dd.csv", key="a12b3c", llm_provider="fish"
        )


def test_init_with_llm():
    # test no errors occur
    writer = DictWriter(api_key="1234", llm_provider="openai")
    assert isinstance(writer.model, OpenAILanguageModel)


def test_reset_headers_and_validate_correct(writer):
    dd = pd.DataFrame(
        {
            "source_field": ["field_1", "field_2", "field_3"],
            "source_description": ["name", "age", "species"],
            "source_type": ["string", "float", "choice"],
            "common_values": [None, None, "cat, dog, fish"],
        }
    )

    dd_new = writer._reset_headers_and_validate(dd)

    assert list(dd_new.columns) == [
        "Field Name",
        "Description",
        "Field Type",
        "Common Values",
    ]


def test_reset_headers_and_validate_duplicates(writer):
    dd = pd.DataFrame(
        {
            "source_field": ["field_1", "field_2", "field_3"],
            "source_description": ["name", "name", "species"],
            "source_type": ["string", "string", "choice"],
            "common_values": [None, None, "cat, dog, fish"],
        }
    )

    with pytest.warns(
        UserWarning, match="Duplicate descriptions found in the data dictionary"
    ):
        dd_new = writer._reset_headers_and_validate(dd)

    assert list(dd_new.columns) == [
        "Field Name",
        "Description",
        "Field Type",
        "Common Values",
    ]


def test_reset_headers_and_validate_duplicate_fields(writer):
    dd = pd.DataFrame(
        {
            "source_field": ["field_1", "field_1", "field_3"],
            "source_description": ["name", "age", "species"],
            "source_type": ["string", "float", "choice"],
            "common_values": [None, None, "cat, dog, fish"],
        }
    )

    with pytest.raises(SchemaError, match="'Field Name' contains duplicate values"):
        writer._reset_headers_and_validate(dd)


def test_main_cli(tmp_path):
    ARGV = [
        SOURCES + "animal_data.csv",
        "fr",
        "-c",
        CONFIG_PATH,
        "-o",
        str(tmp_path / "test_animals_dd"),
    ]

    main(ARGV)

    assert (tmp_path / "test_animals_dd.csv").exists()


def test_main_cli_error_descrip_no_apikey(tmp_path):
    ARGV = [
        SOURCES + "animal_data.csv",
        "fr",
        "-d",
        "-c",
        CONFIG_PATH,
        "-o",
        str(tmp_path / "test_animals_dd"),
    ]

    with pytest.raises(ValueError, match="API key required"):
        main(ARGV)


class DictTest(DictWriter):
    # override the __init__ method to avoid calling any LLM API's, and fill with dummy
    # data from testing_data.py
    def __init__(self, llm_provider=None, api_key=None):
        super().__init__(
            llm_provider,
            api_key,
        )

        self.model = TestLLM()


def test_main_cli_with_descrip(monkeypatch, tmp_path):
    ARGV = [
        SOURCES + "animal_data.csv",
        "fr",
        "-d",
        "-k",
        "1a2s3c4d",
        "-c",
        CONFIG_PATH,
        "-o",
        str(tmp_path / "test_animals_dd"),
    ]

    monkeypatch.setattr("adtl.autoparser.dict_writer.DictWriter", DictTest)

    main(ARGV)

    assert (tmp_path / "test_animals_dd.csv").exists()
