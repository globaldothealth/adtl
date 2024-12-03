"Contains all functions that call OpenAI's API."

from __future__ import annotations


class LLMBase:
    def __init__(self, api_key, model=None):  # pragma: no cover
        self.client = None
        self.model = model

    def get_definitions(self, headers, language):  # pragma: no cover
        """
        Get the definitions of the columns in the dataset.
        """
        # subclasses should implement this method
        raise NotImplementedError

    def map_fields(self, source_fields, target_fields):  # pragma: no cover
        """
        Calls the OpenAI API to generate a draft mapping between two datasets.
        """
        # subclasses should implement this method
        raise NotImplementedError

    def map_values(self, values, language):  # pragma: no cover
        """
        Calls the OpenAI API to generate a set of value mappings for the fields.
        """
        # subclasses should implement this method
        raise NotImplementedError
