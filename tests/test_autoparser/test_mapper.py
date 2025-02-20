# tests the `Mapper` class
from __future__ import annotations

from pathlib import Path

import numpy as np
import numpy.testing as npt
import pandas as pd
import pytest
from testing_data_animals import TestLLM

from adtl.autoparser import create_mapping
from adtl.autoparser.language_models.openai import OpenAILanguageModel
from adtl.autoparser.mapping import Mapper, main


class MapperTest(Mapper):
    # override the __init__ method to avoid calling any LLM API's, and fill with dummy
    # data from testing_data.py
    def __init__(
        self, data_dictionary, schema, language, api_key=None, llm=None, config=None
    ):
        super().__init__(
            data_dictionary,
            schema,
            language,
            None,
            None,
        )

        self.model = TestLLM()


ANIMAL_MAPPER = MapperTest(
    "tests/test_autoparser/sources/animals_dd_described.csv",
    Path("tests/test_autoparser/schemas/animals.schema.json"),
    "fr",
)


def test_target_fields():
    mapper = ANIMAL_MAPPER
    npt.assert_array_equal(
        mapper.target_fields,
        [
            "identity",
            "name",
            "loc_admin_1",
            "country_iso3",
            "notification_date",
            "classification",
            "case_status",
            "date_of_death",
            "age_years",
            "age_months",
            "sex",
            "pet",
            "chipped",
            "owner",
            "underlying_conditions",
        ],
    )


def test_target_types():
    mapper = ANIMAL_MAPPER
    assert mapper.target_types == {
        "identity": ["string", "integer"],
        "name": ["string", "null"],
        "loc_admin_1": ["string", "null"],
        "country_iso3": ["string"],
        "notification_date": ["string", "null"],
        "classification": ["string", "null"],
        "case_status": ["string", "null"],
        "date_of_death": ["string", "null"],
        "age_years": ["number", "null"],
        "age_months": ["number", "null"],
        "sex": ["string", "null"],
        "pet": ["boolean", "null"],
        "chipped": ["boolean", "null"],
        "owner": ["string", "null"],
        "underlying_conditions": ["array", "null"],
    }


def test_target_values():
    mapper = ANIMAL_MAPPER

    target_vals = pd.Series(
        data=[
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            ["mammal", "bird", "reptile", "amphibian", "fish", "invertebrate", None],
            ["alive", "dead", "unknown", None],
            np.nan,
            np.nan,
            np.nan,
            ["male", "female", "other", "unknown", None],
            ["True", "False", "None"],
            ["True", "False", "None"],
            np.nan,
            ["diabetes", "arthritis", "seizures", "vomiting", "skin problems"],
        ],
        index=[
            "identity",
            "name",
            "loc_admin_1",
            "country_iso3",
            "notification_date",
            "classification",
            "case_status",
            "date_of_death",
            "age_years",
            "age_months",
            "sex",
            "pet",
            "chipped",
            "owner",
            "underlying_conditions",
        ],
    )

    pd.testing.assert_series_equal(mapper.target_values, target_vals)


def test_common_values():
    mapper = ANIMAL_MAPPER

    common_vals = pd.Series(
        data=[
            None,
            {"orientale", "katanga", "equateur"},
            None,
            {"poisson", "fish", "rept", "oiseau", "mammifère", "amphibie"},
            None,
            None,
            None,
            None,
            {"m", "f"},
            {"vivant", "décédé"},
            None,
            {"non", "oui"},
            {"non", "oui"},
            {"non", "autres", "voyage"},
            {"non", "oui"},
            {"non", "oui"},
            {"non", "oui"},
            {"diabète", "vomir", "convulsions", "arthrite", "problèmes d'échelle"},
        ],
        index=[
            "Identité",
            "Province",
            "DateNotification",
            "Classicfication ",
            "Nom complet ",
            "Date de naissance",
            "AgeAns",
            "AgeMois         ",
            "Sexe",
            "StatusCas",
            "DateDec",
            "ContSoins ",
            "ContHumain Autre",
            "AutreContHumain",
            "ContactAnimal",
            "Micropucé",
            "AnimalDeCompagnie",
            "ConditionsPreexistantes",
        ],
    )
    pd.testing.assert_series_equal(mapper.common_values, common_vals, check_names=False)


def test_missing_common_values():
    df = pd.DataFrame(
        {
            "source_field": ["test"],
            "source_description": ["test"],
            "source_type": ["test"],
        }
    )

    with pytest.raises(ValueError, match="No common values or choices"):
        MapperTest(
            df,
            Path("tests/test_autoparser/schemas/animals.schema.json"),
            "fr",
        ).common_values


def test_choices_present():
    df = pd.DataFrame(
        {
            "source_field": ["test"],
            "source_description": ["test"],
            "source_type": ["test"],
            "choices": ["test"],
        }
    )

    with pytest.raises(NotImplementedError, match="choices column not yet supported"):
        MapperTest(
            df,
            Path("tests/test_autoparser/schemas/animals.schema.json"),
            "fr",
        ).common_values


def test_mapped_fields_error():
    with pytest.raises(AttributeError):
        ANIMAL_MAPPER.mapped_fields


def test_common_values_mapped_fields_error():
    with pytest.raises(AttributeError):
        ANIMAL_MAPPER.common_values_mapped


def test_mapper_class_init_raises():
    with pytest.raises(ValueError, match="Unsupported LLM provider: fish"):
        Mapper(
            "tests/test_autoparser/sources/animals_dd_described.csv",
            Path("tests/test_autoparser/schemas/animals.schema.json"),
            "fr",
            api_key="1234",
            llm_provider="fish",
        )


def test_mapper_class_init():
    mapper = Mapper(
        "tests/test_autoparser/sources/animals_dd_described.csv",
        Path("tests/test_autoparser/schemas/animals.schema.json"),
        "fr",
        llm_provider=None,
    )

    assert mapper.language == "fr"
    assert mapper.model is None
    npt.assert_array_equal(
        mapper.data_dictionary.columns,
        ["source_field", "source_description", "source_type", "common_values"],
    )


def test_mapper_class_init_with_llm():
    mapper = Mapper(
        "tests/test_autoparser/sources/animals_dd_described.csv",
        Path("tests/test_autoparser/schemas/animals.schema.json"),
        "fr",
        api_key="abcd",
    )

    assert isinstance(mapper.model, OpenAILanguageModel)


def test_match_fields_to_schema_dummy_data():
    mapper = ANIMAL_MAPPER

    df = mapper.match_fields_to_schema()

    assert df.shape == (15, 4)
    npt.assert_array_equal(
        df.columns,
        [
            "source_description",
            "source_field",
            "source_type",
            "common_values",
        ],
    )
    npt.assert_array_equal(
        df.index,
        [
            "identity",
            "name",
            "loc_admin_1",
            "country_iso3",
            "notification_date",
            "classification",
            "case_status",
            "date_of_death",
            "age_years",
            "age_months",
            "sex",
            "pet",
            "chipped",
            "owner",
            "underlying_conditions",
        ],
    )

    case_status = pd.Series(
        data=["Case Status", "StatusCas", "string", "Vivant, Décédé"],
        index=df.columns,
        name="case_status",
    )

    pd.testing.assert_series_equal(case_status, df.loc["case_status"])

    pd.testing.assert_frame_equal(df, mapper.filtered_data_dict)

    # check mapped_values now filled
    pd.testing.assert_series_equal(mapper.mapped_fields, df["source_field"])

    # check the description that was misspelled is now corrected
    assert df.at["age_years", "source_field"] == "AgeAns"
    assert df.at["date_of_death", "source_field"] is np.nan


def test_match_values_to_schema_dummy_data():
    mapper = ANIMAL_MAPPER

    # fill mapper with dummy data mapping the fields
    mapper.match_fields_to_schema()

    df = mapper.match_values_to_schema()

    assert df.shape == (5,)
    assert df["classification"] == {
        "mammifère": "mammal",
        "oiseau": "bird",
        "rept": "reptile",
        "amphibie": "amphibian",
        "fish": "fish",
        "poisson": "fish",
        "autre": None,
    }


def test_class_create_mapping_no_save():
    mapper = ANIMAL_MAPPER

    with pytest.warns(UserWarning):
        df = mapper.create_mapping(save=False)

    assert df.shape == (15, 5)
    assert df.index.name == "target_field"
    npt.assert_array_equal(
        df.columns,
        [
            "source_description",
            "source_field",
            "common_values",
            "target_values",
            "value_mapping",
        ],
    )

    pet_test = pd.Series(
        {
            "source_description": "Pet Animal",
            "source_field": "AnimalDeCompagnie",
            "common_values": "Oui, Non, non",
            "target_values": "True, False, None",
            "value_mapping": "oui=True, non=False",
        },
        name="pet",
    )
    pd.testing.assert_series_equal(df.loc["pet"], pet_test)


@pytest.mark.filterwarnings("ignore:The following schema fields have not been mapped")
def test_class_create_mapping_save(tmp_path):
    mapper = ANIMAL_MAPPER

    file_name = tmp_path / "test_animals_mapping.csv"

    df = mapper.create_mapping(save=True, file_name=str(file_name))

    pet_test = pd.Series(
        {
            "source_description": "Pet Animal",
            "source_field": "AnimalDeCompagnie",
            "common_values": "Oui, Non, non",
            "target_values": "True, False, None",
            "value_mapping": "oui=True, non=False",
        },
        name="pet",
    )
    pd.testing.assert_series_equal(df.loc["pet"], pet_test)

    loaded_file = pd.read_csv(file_name, index_col=0)
    pd.testing.assert_frame_equal(loaded_file, df)


@pytest.mark.filterwarnings("ignore:The following schema fields have not been mapped")
def test_create_mapping(monkeypatch, tmp_path):
    monkeypatch.setattr("adtl.autoparser.mapping.Mapper", MapperTest)

    create_mapping(
        "tests/test_autoparser/sources/animals_dd_described.csv",
        "tests/test_autoparser/schemas/animals.schema.json",
        "fr",
        "1a2b3c4d",
        save=True,
        file_name=str(tmp_path / "test_animals_mapping.csv"),
    )

    assert (tmp_path / "test_animals_mapping.csv").exists()


@pytest.mark.filterwarnings("ignore:The following schema fields have not been mapped")
def test_main_cli(monkeypatch, tmp_path):
    ARGV = [
        "tests/test_autoparser/sources/animals_dd_described.csv",
        "tests/test_autoparser/schemas/animals.schema.json",
        "fr",
        "1a2b3c4d",
        "-o",
        str(tmp_path / "test_animals_mapping.csv"),
    ]

    monkeypatch.setattr("adtl.autoparser.mapping.Mapper", MapperTest)

    main(ARGV)

    assert (tmp_path / "test_animals_mapping.csv").exists()
