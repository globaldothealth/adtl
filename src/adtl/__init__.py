import importlib.metadata

from adtl.parser import Parser
from adtl.python_interface import check_mapping, parse, validate_specification

__all__ = ["Parser", "parse", "check_mapping", "validate_specification"]

__version__ = importlib.metadata.version("adtl")
