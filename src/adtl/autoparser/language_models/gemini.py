"Contains all functions that call Google's Gemini API."

from __future__ import annotations

import json

import google.generativeai as gemini

from adtl.autoparser.util import ColumnDescriptionRequest, MappingRequest, ValuesRequest

from .base_llm import LLMBase


class GeminiLanguageModel(LLMBase):
    def __init__(self, api_key, model: str = "gemini-1.5-flash"):
        gemini.configure(api_key=api_key)
        self.client = gemini.GenerativeModel(model)
        self.model = model

    def get_definitions(self, headers: list[str], language: str) -> dict[str, str]:
        """
        Get the definitions of the columns in the dataset using the Gemini API.
        """
        result = self.client.generate_content(
            [
                (
                    "You are an expert at structured data extraction. "
                    "The following is a list of headers from a data file in "
                    f"{language}, some containing shortened words or abbreviations. "
                    "Translate them to english. "
                    "Return a list of (original header, translation) pairs, using the given structure."  # noqa
                    "Preserve special characters such as accented letters and hyphens."
                ),
                f"{headers}",
            ],
            generation_config=gemini.GenerationConfig(
                response_mime_type="application/json",
                response_schema=ColumnDescriptionRequest,
            ),
        )
        descriptions = ColumnDescriptionRequest.model_validate(
            json.loads(result.text)
        ).field_descriptions
        return descriptions

    def map_fields(
        self, source_fields: list[str], target_fields: list[str]
    ) -> MappingRequest:
        """
        Calls the Gemini API to generate a draft mapping between two datasets.
        """
        result = self.client.generate_content(
            [
                (
                    "You are an expert at structured data extraction. "
                    "You will be given two lists of phrases, one is the headers for a "
                    "target data file, and the other a set of descriptions for columns "
                    "of source data. "
                    "Match each target header to the best matching source description, "
                    "but match a header to None if a good match does not exist. "
                    "Preserve special characters such as accented letters and hyphens."
                    "Return the matched target headers and source descriptions using the provided structure."  # noqa
                ),
                (
                    f"These are the target headers: {target_fields}\n"
                    f"These are the source descriptions: {source_fields}"
                ),
            ],
            generation_config=gemini.GenerationConfig(
                response_mime_type="application/json",
                response_schema=MappingRequest,
            ),
        )
        return MappingRequest.model_validate(json.loads(result.text))

    def map_values(
        self, values: list[tuple[str, set[str], list[str | None] | None]], language: str
    ) -> ValuesRequest:
        """
        Calls the Gemini API to generate a set of value mappings for the fields.
        """
        result = self.client.generate_content(
            [
                (
                    "You are an expert at structured data extraction. "
                    "You will be given a list of tuples, where each tuple contains "
                    "three sets of string values. "
                    "The first set contains field names for a dataset."
                    "The second set contains values from a source dataset in "
                    f"{language}, and the third set contains target values for an "
                    "english-language transformed dataset. "
                    "Match all the values in the second set to the appropriate values "
                    "in the third set. "
                    "Return a list of dictionaries, where each dictionary contains the "
                    "field name as a key, and a dictionary containing "
                    "source values as keys, and the target text as values, "
                    "as the values. For example, the result should look like this: "
                    "[{'field_name_1': {'source_value_a': 'target_value_a', "
                    "'source_value_b': 'target_value_b'}, 'field_name_2':{...}]"
                    "using the provided structure."
                    "Preserve special characters such as accented letters and hyphens."
                ),
                f"These are the field, source, target value sets: {values}",
            ],
            generation_config=gemini.GenerationConfig(
                response_mime_type="application/json",
                response_schema=ValuesRequest,
            ),
        )
        return ValuesRequest.model_validate(json.loads(result.text))
