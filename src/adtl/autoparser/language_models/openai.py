"Contains all functions that call OpenAI's API."

from __future__ import annotations

from openai import OpenAI
from pydantic import create_model

from .base_llm import LLMBase
from .data_structures import ColumnDescriptionRequest, MappingRequest, ValuesRequest


class OpenAILanguageModel(LLMBase):
    def __init__(self, api_key, model: str = "gpt-4o-mini"):
        self.client = OpenAI(api_key=api_key)
        self.model = model

        if self.model not in self.valid_models():
            raise ValueError(
                f"Unsupported OpenAI model. Must be one of {self.valid_models}."
            )

    @classmethod
    def valid_models(cls):
        return ["gpt-4o-mini", "gpt-4o", "o1", "o3-mini"]

    def get_definitions(self, headers: list[str], language: str) -> dict[str, str]:
        """
        Get the definitions of the columns in the dataset.
        """
        completion = self.client.beta.chat.completions.parse(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an expert at structured data extraction. "
                        "The following is a list of headers from a data file in "
                        f"{language}, some containing shortened words or abbreviations. "  # noqa
                        "Translate them to english. "
                        "Return a list of (original header, translation) pairs, using the given structure."  # noqa
                    ),
                },
                {"role": "user", "content": f"{headers}"},
            ],
            response_format=ColumnDescriptionRequest,
        )
        descriptions = completion.choices[0].message.parsed.field_descriptions

        return descriptions

    def map_fields(
        self, source_fields: list[str], target_fields: list[str]
    ) -> MappingRequest:
        """
        Calls the OpenAI API to generate a draft mapping between two datasets.
        """
        field_mapping = self.client.beta.chat.completions.parse(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an expert at structured data extraction. "
                        "You will be given two lists of phrases, one is the headers "
                        "for a target data file, and the other a set of descriptions "
                        "for columns of source data. "
                        "Match each target header to the best matching source "
                        "description, but match a header to None if a good match does "
                        "not exist. "
                        "Return the matched target headers and source descriptions using the provided structure."  # noqa
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"These are the target headers: {target_fields}\n"
                        f"These are the source descriptions: {source_fields}"
                    ),
                },
            ],
            response_format=MappingRequest,
        )
        mappings = field_mapping.choices[0].message.parsed

        return mappings

    def map_values(
        self, values: list[tuple[str, set[str], list[str | None] | None]], language: str
    ) -> ValuesRequest:
        """
        Calls the OpenAI API to generate a set of value mappings for the fields.
        """
        value_mapping = self.client.beta.chat.completions.parse(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an expert at structured data extraction. "
                        "You will be given a list of tuples, where each tuple contains "
                        "three sets of string values. "
                        "The first set contains field names for a dataset."
                        "The second set contains values from a source dataset in "
                        f"{language}, and the third set contains target values for an "
                        "english-language transformed dataset. "
                        "Match all the values in the second set to the appropriate "
                        "values in the third set. "
                        "Return a list of dictionaries, where each dictionary contains "
                        "the field name as a key, and a dictionary containing "
                        "source values as keys, and the target text as values, "
                        "as the values. For example, the result should look like this: "
                        "[{'field_name_1': {'source_value_a': 'target_value_a', "
                        "'source_value_b': 'target_value_b'}, 'field_name_2':{...}]"
                        "using the provided structure."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"These are the field, source, target value sets: {values}"
                    ),
                },
            ],
            response_format=ValuesRequest,
        )
        mappings = value_mapping.choices[0].message.parsed

        return mappings

    def map_long_table(self, single_field_format, descriptions, enums):
        """
        Calls the OpenAI API to generate a mapping for a long table.
        """

        LongTableRequest = create_model(
            "LongTableRequest", long_table=(list[single_field_format], ...)
        )

        system_msg_descriptive = """
            You are an expert at structured data extraction.
            For each source column description provided, match it to the best variable name
            from the given list of enums. If no good match exists, use null.

            Return output as a JSON array of objects, each with fields:
            - source_description (string)
            - variable_name (string or null)
            - value_col (one of "value_bool", "value_num", "value" or null)
            - phase (one of "presentation", "outcome", or null)
            - attribute_unit (string or null)

            Example input:

            Columns descriptions: ["Age", "Symptoms: Cough", "Admission Date"]
            Variable names: ["age", "cough", "aids_hiv"]

            Example output:
            [
            {"source_description": "Age", "variable_name": "age", "value_col": "value_num", "phase": "presentation", "attribute_unit": "years"},
            {"source_description": "Symptoms: Cough", "variable_name": "cough", "value_col": "value_bool", "phase": "presentation", "attribute_unit": ""},
            {"source_description": "Admission Date", "variable_name": null, "value_col": "", "phase": "", "attribute_unit": ""}
            ]
            """

        long_table_mapping = self.client.beta.chat.completions.parse(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": system_msg_descriptive,
                },
                {
                    "role": "user",
                    "content": (
                        f"Columns descriptions: {', '.join(descriptions)}"
                        f"Variable names: {', '.join(enums)}"
                    ),
                },
            ],
            response_format=LongTableRequest,
        )
        mappings = long_table_mapping.choices[0].message.parsed

        return mappings
