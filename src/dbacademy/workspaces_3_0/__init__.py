"""
This module serves as the basis for the scripts that provision the "Workspace 3.0" set of Databricks Workspaces.
"""
from typing import Any


class ParameterValidator:
    def __init__(self):
        pass

    @staticmethod
    def validate_type(parameter_type: Any, **kwargs):
        assert len(kwargs) == 1, f"validate_type() expects two and only two parameters."
        for parameter_name, value in kwargs.items():
            assert type(value) == parameter_type, f"""The parameter "{parameter_name}" must be of type {parameter_type}, found {type(value)}."""

    @staticmethod
    def validate_min_length(parameter_name: str, length: int, value: Any):
        if type(value) in [list, str]:
            assert len(value) > 0, f"""The parameter "{parameter_name}" have a length of at least {length}, found "{value}"."""
        else:
            raise Exception(f"""Cannot determine the parameter "{parameter_name}"'s length for object of type {type(value)}""")

    @staticmethod
    def validate_not_none(**kwargs):
        assert len(kwargs) == 1, f"validate_not_none() expects one and only one parameter."
        for parameter_name, value in kwargs.items():
            assert value is not None, f"""The parameter "{parameter_name}" must be specified."""
