# tests the `DictWriter` class
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from testing_data_animals import TestLLM

import adtl.autoparser as autoparser
from adtl.autoparser.dict_writer import DictWriter, main
from adtl.autoparser.language_models.openai import OpenAILanguageModel

CONFIG_PATH = "tests/test_autoparser/test_config.toml"
SOURCES = "tests/test_autoparser/sources/"
SCHEMAS = "tests/test_autoparser/schemas/"


def test_unsupported_data_format_txt():
    writer = DictWriter(config=CONFIG_PATH)

    with pytest.raises(ValueError, match="Unsupported format"):
        writer.create_dict(SOURCES + "animals.txt")


def test_data_not_df_or_path():
    writer = DictWriter(config=CONFIG_PATH)

    with pytest.raises(ValueError, match="Data must be a path"):
        writer.create_dict(None)


def test_dictionary_creation_no_descrip():
    writer = DictWriter(config=CONFIG_PATH)

    df = writer.create_dict(SOURCES + "animal_data.csv")

    df_desired = pd.read_csv(SOURCES + "animals_dd.csv")

    pd.testing.assert_frame_equal(df, df_desired)


def test_create_dict_no_descrip():
    df = autoparser.create_dict(SOURCES + "animal_data.csv", config=CONFIG_PATH)

    df_desired = pd.read_csv(SOURCES + "animals_dd.csv")

    pd.testing.assert_frame_equal(df, df_desired)


def test_dictionary_creation_no_descrip_excel_dataframe():
    writer = DictWriter(config=CONFIG_PATH)

    # check no errors excel
    writer.create_dict(SOURCES + "animal_data.xlsx")

    # check no errors dataframe
    df = pd.read_csv(SOURCES + "animals_dd.csv")
    writer.create_dict(df)


def test_dictionary_description():
    writer = DictWriter(config=Path(CONFIG_PATH))
    writer.model = TestLLM()

    # check descriptions aren't generated without a dictionary
    with pytest.raises(ValueError, match="No data dictionary found"):
        writer.generate_descriptions("fr")

    df = writer.generate_descriptions("fr", SOURCES + "animals_dd.csv")

    df_desired = pd.read_csv(SOURCES + "animals_dd_described.csv")

    pd.testing.assert_frame_equal(df, df_desired)


def test_missing_key_error():
    with pytest.raises(ValueError, match="API key required"):
        DictWriter(config=Path(CONFIG_PATH)).generate_descriptions(
            "fr", SOURCES + "animals_dd.csv"
        )


def test_wrong_llm_error():
    with pytest.raises(ValueError, match="Unsupported LLM provider: fish"):
        DictWriter(config=Path(CONFIG_PATH)).generate_descriptions(
            "fr", SOURCES + "animals_dd.csv", key="a12b3c", llm="fish"
        )


def test_init_with_llm():
    # test no errors occur
    writer = DictWriter(config=Path(CONFIG_PATH), api_key="1234", llm="openai")
    assert isinstance(writer.model, OpenAILanguageModel)


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
    def __init__(self, config, llm=None, api_key=None):
        super().__init__(
            config,
            llm,
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
