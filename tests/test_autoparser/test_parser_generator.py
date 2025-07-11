from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest
import tomli

import adtl.autoparser as autoparser
from adtl.autoparser import ParserGenerator, setup_config
from adtl.autoparser import make_toml_main as main
from adtl.autoparser.make_toml import WideTableParser


@pytest.fixture(autouse=True)
def config():
    """Fixture to load the configuration for the autoparser."""
    setup_config(
        {
            "name": "test_autoparser",
            "language": "en",
            "max_common_count": 8,
            "schemas": {"animals": "tests/test_autoparser/schemas/animals.schema.json"},
        }
    )


@pytest.fixture
def wide_parser():
    with open("tests/test_autoparser/schemas/animals.schema.json", "r") as f:
        schema = json.load(f)

    return WideTableParser(
        mapping=pd.read_csv("tests/test_autoparser/sources/animals_mapping.csv"),
        schema=schema,
        table_name="animals",
    )


def test_constant_fields(wide_parser):
    expected = {
        "identity": False,
        "name": False,
        "loc_admin_1": False,
        "country_iso3": False,
        "notification_date": False,
        "classification": False,
        "case_status": False,
        "date_of_death": False,
        "age_years": False,
        "age_months": False,
        "sex": False,
        "pet": False,
        "chipped": False,
        "owner": False,
        "underlying_conditions": False,
    }

    assert wide_parser.constant_field == expected


def test_update_constant_fields(wide_parser):
    with pytest.raises(ValueError, match="is not a valid schema field"):
        wide_parser.update_constant_fields({"unknown_field": True})

    with pytest.raises(ValueError, match="must be True or False"):
        wide_parser.update_constant_fields({"name": "Betty"})

    wide_parser.update_constant_fields({"country_iso3": True})

    assert wide_parser.constant_field["country_iso3"] is True


def test_parsed_choices(wide_parser):
    choices = pd.Series(
        data=[
            None,
            None,
            None,
            None,
            None,
            {
                "mammifère": "mammal",
                "fish": "fish",
                "poisson": "fish",
                "amphibie": "amphibian",
                "oiseau": "bird",
                "autre": "",
                "rept": "reptile",
            },
            {"vivant": "alive", "décédé": "dead"},
            None,
            None,
            None,
            {"m": "male", "f": "female"},
            {"oui": True, "non": False},
            {"oui": True, "non": False},
            None,
            {
                "problèmes d'échelle": "skin problems",
                "convulsions": "seizures",
                "diabète": "diabetes",
                "vomir": "vomiting",
                "arthrite": "arthritis",
            },
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

    pd.testing.assert_series_equal(
        choices, wide_parser.parsed_choices, check_names=False
    )


def test_references_definitions(wide_parser):
    ref_def = (
        {'{"non": false, "oui": true}': "Y/N/NK"},
        {"Y/N/NK": {"caseInsensitive": True, "values": {"oui": True, "non": False}}},
    )

    assert wide_parser.references_definitions == ref_def


s1 = pd.Series(
    {
        "classification": {
            "mammifère": "mammal",
            "fish": "fish",
            "amphibie": "amphibian",
        },
        "case_status": {"vivant": "alive", "decede": "dead"},
        "another_status": {"vivant": "alive", "decede": "dead"},
        "pet": {"oui": True, "non": False},
        "chipped": {"oui": True, "non": False},
        "vaccinated": {"oui": True, "non": "pending"},
        "spayed": {"oui": True, "non": "pending"},
    }
)


@pytest.mark.parametrize(
    "source, expected",
    [
        (
            s1,
            (
                {
                    '{"decede": "dead", "vivant": "alive"}': "alive/dead",
                    '{"non": false, "oui": true}': "Y/N/NK",
                },
                {
                    "alive/dead": {
                        "caseInsensitive": True,
                        "values": {"decede": "dead", "vivant": "alive"},
                    },
                    "Y/N/NK": {
                        "caseInsensitive": True,
                        "values": {"oui": True, "non": False},
                    },
                },
            ),
        )
    ],
)
def test_ref_def(source, expected, wide_parser):
    choices = source.value_counts()

    # provide a different dataset than the one in the class
    answer = wide_parser.refs_defs(choices, 3)

    assert answer == expected


def test_schema_fields(wide_parser, snapshot):
    assert wide_parser.schema_fields == snapshot


@pytest.mark.parametrize(
    "row, expected",
    [
        (
            pd.Series(
                data=["age_months", "Age in Months", "AgeMois", np.nan, np.nan, np.nan],
                index=[
                    "target_field",
                    "source_description",
                    "source_field",
                    "common_values",
                    "target_values",
                    "value_mapping",
                ],
            ),
            {"field": "AgeMois", "description": "Age in Months"},
        ),
        (
            pd.Series(
                data=[
                    "case_status",
                    "Case Status",
                    "StatusCas",
                    "Vivant, Décédé",
                    "alive, dead, unknown, None",
                    "vivant=alive, décédé=dead",
                ],
                index=[
                    "target_field",
                    "source_description",
                    "source_field",
                    "common_values",
                    "target_values",
                    "value_mapping",
                ],
            ),
            {
                "field": "StatusCas",
                "description": "Case Status",
                "values": {"vivant": "alive", "décédé": "dead"},
                "caseInsensitive": True,
            },
        ),
        (
            pd.Series(
                data=[
                    "pet",
                    "Pet Animal",
                    "AnimalDeCompagnie",
                    "Non, non, Oui",
                    "True, False, None",
                    "oui=True, non=False",
                ],
                index=[
                    "target_field",
                    "source_description",
                    "source_field",
                    "common_values",
                    "target_values",
                    "value_mapping",
                ],
            ),
            {
                "field": "AnimalDeCompagnie",
                "description": "Pet Animal",
                "ref": "Y/N/NK",
            },
        ),
    ],
)
def test_single_field_mapping(row, expected, wide_parser):
    assert wide_parser.single_field_mapping(row) == expected


def test_create_parser(tmp_path, snapshot):
    parser = ParserGenerator(
        "tests/test_autoparser/sources/animals_mapping.csv",
        "",
        "animals",
    )

    file = tmp_path / "test.toml"

    parser.create_parser(file_name=file)

    with file.open("rb") as fp:
        parser_file = tomli.load(fp)

    # check body of parser file
    assert parser_file["animals"] == snapshot


def test_create_parser_ap_access(tmp_path, snapshot):
    file = tmp_path / "test.toml"

    autoparser.create_parser(
        "tests/test_autoparser/sources/animals_mapping.csv",
        "",
        str(file),
    )

    with file.open("rb") as fp:
        parser_file = tomli.load(fp)

    # check body of parser file
    assert parser_file["animals"] == snapshot


def test_main_cli(tmp_path):
    ARGV = [
        "tests/test_autoparser/sources/animals_mapping.csv",
        "",
        "-o",
        str(tmp_path / "animals"),
        "-c",
        "tests/test_autoparser/test_config.toml",
    ]

    main(ARGV)

    assert (tmp_path / "animals.toml").exists()
