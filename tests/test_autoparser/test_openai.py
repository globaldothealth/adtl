"Tests the OpenAILanguageModel class."

import datetime

import pytest
from openai.types.chat.parsed_chat_completion import (
    ParsedChatCompletion,
    ParsedChatCompletionMessage,
    ParsedChoice,
)
from testing_data_animals import (
    SingleEntry,
    get_definitions,
    map_fields,
    map_long_table,
    map_values,
)

from adtl.autoparser.language_models.data_structures import ColumnDescriptionRequest
from adtl.autoparser.language_models.openai import OpenAILanguageModel


def test_init():
    model = OpenAILanguageModel("1234")

    assert model.client is not None
    assert model.model == "gpt-4o-mini"


def test_init_invalid_model_raises():
    with pytest.raises(ValueError, match="Unsupported OpenAI model. Must be one of"):
        OpenAILanguageModel("1234", model="invalid_model")


def test_init_with_model():
    model = OpenAILanguageModel("1234", model="o3-mini")
    assert model.model == "o3-mini"


def test_get_definitions(monkeypatch):
    model = OpenAILanguageModel("1234")

    # Define test inputs
    headers = ["foo", "bar", "baz"]
    language = "fr"

    # Define the mocked response
    def mock_parse(*args, **kwargs):
        return ParsedChatCompletion(
            id="foo",
            model="gpt-4o-mini",
            object="chat.completion",
            choices=[
                ParsedChoice(
                    message=ParsedChatCompletionMessage(
                        content='{"field_descriptions":[{"field_name":"Identité","translation":"Identity"},{"field_name":"Province","translation":"Province"},{"field_name":"DateNotification","translation":"Notification Date"},{"field_name":"Classicfication ","translation":"Classification"},{"field_name":"Nom complet ","translation":"Full Name"},{"field_name":"Date de naissance","translation":"Date of Birth"},{"field_name":"AgeAns","translation":"Age Years"},{"field_name":"AgeMois         ","translation":"Age Months"},{"field_name":"Sexe","translation":"Sex"},{"field_name":"StatusCas","translation":"Case Status"},{"field_name":"DateDec","translation":"Date of Death"},{"field_name":"ContSoins ","translation":"Care Contact"},{"field_name":"ContHumain Autre","translation":"Other Human Contact"},{"field_name":"AutreContHumain","translation":"Other Human Contact"},{"field_name":"ContactAnimal","translation":"Animal Contact"},{"field_name":"Micropucé","translation":"Microchipped"},{"field_name":"AnimalDeCompagnie","translation":"Pet"}]}',  # noqa
                        role="assistant",
                        parsed=ColumnDescriptionRequest(
                            field_descriptions=get_definitions()
                        ),
                    ),
                    finish_reason="stop",
                    index=0,
                )
            ],
            created=int(datetime.datetime.now().timestamp()),
        )

    # Mock the parse method using monkeypatch
    monkeypatch.setattr(model.client.beta.chat.completions, "parse", mock_parse)

    # Call the function
    result = model.get_definitions(headers, language)

    # Assert the expected output
    assert result == get_definitions()


def test_map_fields(monkeypatch):
    model = OpenAILanguageModel("1234")

    # Define test inputs
    source_fields = ["nom", "âge", "localisation"]
    target_fields = ["name", "age", "location"]

    # Define the mocked response
    def mock_parse(*args, **kwargs):
        return ParsedChatCompletion(
            id="foo",
            model="gpt-4o-mini",
            object="chat.completion",
            choices=[
                ParsedChoice(
                    message=ParsedChatCompletionMessage(
                        content="",  # noqa
                        role="assistant",
                        parsed=map_fields(),
                    ),
                    finish_reason="stop",
                    index=0,
                )
            ],
            created=int(datetime.datetime.now().timestamp()),
        )

    # Mock the parse method using monkeypatch
    monkeypatch.setattr(model.client.beta.chat.completions, "parse", mock_parse)

    # Call the function
    result = model.map_fields(source_fields, target_fields)

    # Assert the expected output
    assert result == map_fields()


def test_map_values(monkeypatch):
    model = OpenAILanguageModel("1234")

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
    def mock_parse(*args, **kwargs):
        return ParsedChatCompletion(
            id="foo",
            model="gpt-4o-mini",
            object="chat.completion",
            choices=[
                ParsedChoice(
                    message=ParsedChatCompletionMessage(
                        content="",  # noqa
                        role="assistant",
                        parsed=map_values(),
                    ),
                    finish_reason="stop",
                    index=0,
                )
            ],
            created=int(datetime.datetime.now().timestamp()),
        )

    # Mock the parse method using monkeypatch
    monkeypatch.setattr(model.client.beta.chat.completions, "parse", mock_parse)

    # Call the function
    result = model.map_values(values, "fr")

    # Assert the expected output
    assert result == map_values()


def test_map_long_table(monkeypatch):
    model = OpenAILanguageModel("1234")

    # Define test inputs
    descriptions = [
        "Weight in kg",
        "Vaccination Status",
        "Reported issues",
        "Temperature in Celsius",
    ]
    enums = ["weight", "vaccinated", "behavioural_issue", "temperature"]

    # Define the mocked response
    def mock_parse(*args, **kwargs):
        return ParsedChatCompletion(
            id="foo",
            model="gpt-4o-mini",
            object="chat.completion",
            choices=[
                ParsedChoice(
                    message=ParsedChatCompletionMessage(
                        content="",  # noqa
                        role="assistant",
                        parsed=map_long_table(),
                    ),
                    finish_reason="stop",
                    index=0,
                )
            ],
            created=int(datetime.datetime.now().timestamp()),
        )

    # Mock the parse method using monkeypatch
    monkeypatch.setattr(model.client.beta.chat.completions, "parse", mock_parse)

    # Call the function
    result = model.map_long_table(SingleEntry, descriptions, enums)

    # Assert the expected output
    expected = map_long_table()
    assert expected.model_dump(mode="json") == result.model_dump(mode="json")
