"Contains all functions that call OpenAI's API."

from __future__ import annotations

from openai import OpenAI

from adtl.autoparser.util import ColumnDescriptionRequest, MappingRequest, ValuesRequest

from .base_llm import LLMBase


class OpenAILanguageModel(LLMBase):
    def __init__(self, api_key, model: str = "gpt-4o-mini"):
        self.client = OpenAI(api_key=api_key)
        self.model = model

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
