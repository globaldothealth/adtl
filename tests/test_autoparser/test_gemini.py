"Tests the GeminiLanguageModel class."

import pytest
from google.genai.types import Candidate, Content, GenerateContentResponse, Part
from testing_data_animals import get_definitions, map_fields, map_values

from adtl.autoparser.language_models.gemini import GeminiLanguageModel


def test_init():
    model = GeminiLanguageModel("1234")

    assert model.client is not None
    assert model.model == "gemini-2.5-flash"


def test_init_invalid_model_raises():
    with pytest.raises(ValueError, match="Unsupported Gemini model. Must be one of"):
        GeminiLanguageModel("1234", model="invalid_model")


def test_init_with_model():
    model = GeminiLanguageModel("1234", model="gemini-2.0-flash")
    assert model.model == "gemini-2.0-flash"


def test_get_definitions(monkeypatch):
    model = GeminiLanguageModel("1234")

    # Define test inputs
    headers = ["foo", "bar", "baz"]
    language = "fr"

    # Define the mocked response
    def mock_generate_content(*args, **kwargs):
        json_str = '{"field_descriptions": [{"field_name": "Identité", "translation": "Identity"}, {"field_name": "Province", "translation": "Province"}, {"field_name": "DateNotification", "translation": "Notification Date"}, {"field_name": "Classicfication ", "translation": "Classification"}, {"field_name": "Nom complet ", "translation": "Full Name"}, {"field_name": "Date de naissance", "translation": "Date of Birth"}, {"field_name": "AgeAns", "translation": "Age in Years"}, {"field_name": "AgeMois         ", "translation": "Age in Months"}, {"field_name": "Sexe", "translation": "Gender"}, {"field_name": "StatusCas", "translation": "Case Status"}, {"field_name": "DateDec", "translation": "Date of Death"}, {"field_name": "ContSoins ", "translation": "Care Contact"}, {"field_name": "ContHumain Autre", "translation": "Other Human Contact"}, {"field_name": "ContexteContHumain", "translation": "Human Contact Context"}, {"field_name": "ContactAnimal", "translation": "Animal Contact"}, {"field_name": "Micropucé", "translation": "Microchipped"}, {"field_name": "AnimalDeCompagnie", "translation": "Pet Animal"}, {"field_name": "ConditionsPreexistantes", "translation": "Preexisting Conditions"}]}'  # noqa
        return GenerateContentResponse(
            candidates=[
                Candidate(
                    content=Content(parts=[Part(text=json_str)], role="model"),
                    finish_reason="STOP",
                )
            ]
        )

    # Mock the parse method using monkeypatch
    monkeypatch.setattr(model.client.models, "generate_content", mock_generate_content)

    # Call the function
    result = model.get_definitions(headers, language)

    # Assert the expected output
    assert result == get_definitions()


def test_map_fields(monkeypatch):
    model = GeminiLanguageModel("1234")

    # Define test inputs
    source_fields = ["nom", "âge", "localisation"]
    target_fields = ["name", "age", "location"]

    # Define the mocked response
    def mock_generate_content(*args, **kwargs):
        json_str = '{"targets_descriptions": [{"source_description": "Identity", "target_field": "identity"}, {"source_description": "Full Name", "target_field": "name"}, {"source_description": "Province", "target_field": "loc_admin_1"}, {"source_description": null, "target_field": "country_iso3"}, {"source_description": "Notification Date", "target_field": "notification_date"}, {"source_description": "Classification", "target_field": "classification"}, {"source_description": "Case Status", "target_field": "case_status"}, {"source_description": "Death Date", "target_field": "date_of_death"}, {"source_description": "Age Years", "target_field": "age_years"}, {"source_description": "Age in Months", "target_field": "age_months"}, {"source_description": "Gender", "target_field": "sex"}, {"source_description": "Pet Animal", "target_field": "pet"}, {"source_description": "Microchipped", "target_field": "chipped"}, {"source_description": null, "target_field": "owner"}, {"source_description": "Preexisting Conditions", "target_field": "underlying_conditions"}]}'  # noqa
        return GenerateContentResponse(
            candidates=[
                Candidate(
                    content=Content(parts=[Part(text=json_str)], role="model"),
                    finish_reason="STOP",
                )
            ]
        )

    # Mock the parse method using monkeypatch
    monkeypatch.setattr(model.client.models, "generate_content", mock_generate_content)

    # Call the function
    result = model.map_fields(source_fields, target_fields)

    # Assert the expected output
    assert result == map_fields()


def test_map_values(monkeypatch):
    model = GeminiLanguageModel("1234")

    # Define test inputs
    fields = ["loc", "status", "pet"]
    source_values = [
        {"orientale", "katanga", "kinshasa", "equateur"},
        {"vivant", "décédé"},
        {"oui", "non"},
    ]
    target_values = [
        None,
        ["alive", "dead", "unknown", None],
        ["True", "False", "None"],
    ]
    values = list(zip(fields, source_values, target_values))

    # Define the mocked response
    def mock_generate_content(*args, **kwargs):
        json_str = '{"values": [{"field_name": "classification", "mapped_values": [{"source_value": "mammifère", "target_value": "mammal"}, {"source_value": "fish", "target_value": "fish"}, {"source_value": "poisson", "target_value": "fish"}, {"source_value": "amphibie", "target_value": "amphibian"}, {"source_value": "oiseau", "target_value": "bird"}, {"source_value": "autre", "target_value": null}, {"source_value": "rept", "target_value": "reptile"}]}, {"field_name": "case_status", "mapped_values": [{"source_value": "vivant", "target_value": "alive"}, {"source_value": "décédé", "target_value": "dead"}]}, {"field_name": "sex", "mapped_values": [{"source_value": "m", "target_value": "male"}, {"source_value": "f", "target_value": "female"}, {"source_value": "inconnu", "target_value": null}]}, {"field_name": "pet", "mapped_values": [{"source_value": "oui", "target_value": "True"}, {"source_value": "non", "target_value": "False"}]}, {"field_name": "chipped", "mapped_values": [{"source_value": "oui", "target_value": "True"}, {"source_value": "non", "target_value": "False"}]}]}'  # noqa
        return GenerateContentResponse(
            candidates=[
                Candidate(
                    content=Content(parts=[Part(text=json_str)], role="model"),
                    finish_reason="STOP",
                )
            ]
        )

    # Mock the parse method using monkeypatch
    monkeypatch.setattr(model.client.models, "generate_content", mock_generate_content)

    # Call the function
    result = model.map_values(values, "fr")

    # Assert the expected output
    assert result == map_values()
