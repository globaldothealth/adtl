"""Contains functions which override LLM calls with dummy data for testing purposes."""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, create_model

from adtl.autoparser.language_models.base_llm import LLMBase
from adtl.autoparser.language_models.data_structures import (
    ColumnDescriptionRequest,
    FieldMapping,
    MappingRequest,
    SingleField,
    SingleMapping,
    ValueMapping,
    ValuesRequest,
)

descriptions = {
    "Identité": "Identity",
    "Province": "Province",
    "DateNotification": "Notification Date",
    "Classicfication ": "Classification",
    "Nom complet ": "Full Name",
    "Date de naissance": "Date of Birth",
    "AgeAns": "Age in Years",
    "AgeMois         ": "Age in Months",
    "Sexe": "Gender",
    "StatusCas": "Case Status",
    "DateDec": "Date of Death",
    "ContSoins ": "Care Contact",
    "ContHumain Autre": "Other Human Contact",
    "ContexteContHumain": "Human Contact Context",
    "ContactAnimal": "Animal Contact",
    "Micropucé": "Microchipped",
    "AnimalDeCompagnie": "Pet Animal",
    "ConditionsPreexistantes": "Preexisting Conditions",
}


def get_definitions(*args):
    translated_fields = []

    for k, v in descriptions.items():
        translated_fields.append(SingleField(field_name=k, translation=v))

    descrip = ColumnDescriptionRequest(field_descriptions=translated_fields)
    return descrip.field_descriptions


field_mapping = {
    "identity": "Identity",
    "name": "Full Name",
    "loc_admin_1": "Province",
    "country_iso3": None,
    "notification_date": "Notification Date",
    "classification": "Classification",
    "case_status": "Case Status",
    "date_of_death": "Death Date",  # "Date of Death", misspelled by 'LLM'
    "age_years": "Age Years",  # "Age in Years", misspelled by 'LLM'
    "age_months": "Age in Months",
    "sex": "Gender",
    "pet": "Pet Animal",
    "chipped": "Microchipped",
    "owner": None,
    "underlying_conditions": "Preexisting Conditions",
}


def map_fields(*args):
    fm = []

    for k, v in field_mapping.items():
        fm.append(SingleMapping(target_field=k, source_description=v))

    mapping = MappingRequest(targets_descriptions=fm)
    return mapping


value_pairs = [
    {
        "classification": {
            "mammifère": "mammal",
            "fish": "fish",
            "poisson": "fish",
            "amphibie": "amphibian",
            "oiseau": "bird",
            "autre": None,
            "rept": "reptile",
        }
    },
    {"case_status": {"vivant": "alive", "décédé": "dead"}},
    {"sex": {"m": "male", "f": "female", "inconnu": None}},
    {
        "pet": {
            "oui": "True",
            "non": "False",
        }
    },
    {
        "chipped": {
            "oui": "True",
            "non": "False",
        }
    },
]


def map_values(*args):
    vm = []

    for vp in value_pairs:
        for k, v in vp.items():
            fm = FieldMapping(
                field_name=k,
                mapped_values=[
                    ValueMapping(source_value=k1, target_value=v1)
                    for k1, v1 in v.items()
                ],
            )
            vm.append(fm)

    mapping = ValuesRequest(values=vm)
    return mapping


long_mapping = [
    {
        "source_description": "Weight in kg",
        "variable_name": "weight",
        "value_col": "numeric_value",
        "vet_name": "Dr. Lopez",
    },
    {
        "source_description": "Vaccination Status",
        "variable_name": "vaccinated",
        "value_col": "boolean_value",
        "vet_name": "Dr. Lopez",
    },
    {
        "source_description": "Reported issues",
        "variable_name": "behavioural_issue",
        "value_col": "string_value",
        "vet_name": "Dr. Lopez",
    },
    {
        "source_description": "Temperature in Celsius",
        "variable_name": "temperature",
        "value_col": "numeric_value",
        "vet_name": "Dr. Kamau",
    },
]


def map_long_table(*args):
    """
    Dummy function to simulate mapping a long table.
    """
    fields = {
        "source_description": (str, ...),
        "variable_name": (
            Optional[
                Enum(
                    "VarColEnum",
                    {
                        v.upper(): v
                        for v in [
                            "weight",
                            "temperature",
                            "vaccinated",
                            "neutered",
                            "pregnant",
                            "arthritis",
                            "behavioural_issue",
                        ]
                    },
                )
            ],
            None,
        ),
        "value_col": (
            Optional[
                Enum(
                    "ValueColEnum",
                    {
                        v.upper(): v
                        for v in ["string_value", "boolean_value", "numeric_value"]
                    },
                )
            ],
            None,
        ),
        "vet_name": (Optional[str], None),
    }
    SingleEntry = create_model("SingleEntry", **fields)

    class LongTableRequest(BaseModel):
        long_table: list[SingleEntry]

    fm = []

    for i in long_mapping:
        fm.append(SingleEntry.model_validate(i))

    mapping = LongTableRequest(long_table=fm)
    return mapping


long_value_mapping = ValuesRequest(
    values=[
        FieldMapping(
            field_name="vaccinated",
            mapped_values=[
                ValueMapping(source_value="true", target_value="True"),
                ValueMapping(source_value="false", target_value="False"),
            ],
        )
    ]
)


class TestLLM(LLMBase):
    __test__ = False  # Prevent pytest from collecting this class

    def __init__(self):
        self.client = None
        self.model = None

    def get_definitions(self, headers, language):
        """
        Get the definitions of the columns in the dataset.
        """
        translated_fields = get_definitions(headers, language)
        return translated_fields

    def map_fields(self, source_fields, target_fields):
        """
        Calls the OpenAI API to generate a draft mapping between two datasets.
        """
        mapping = map_fields(source_fields, target_fields)
        return mapping

    def map_values(self, values, language):
        """
        Calls the OpenAI API to generate a set of value mappings for the fields.
        """
        value_mapping = map_values(values, language)
        return value_mapping

    def map_long_table(self, data_dictionary, table_name, api_key, config=None):
        """
        Calls the OpenAI API to generate a mapping for a long table.
        """
        mapping = map_long_table(data_dictionary, table_name, api_key, config)
        return mapping
