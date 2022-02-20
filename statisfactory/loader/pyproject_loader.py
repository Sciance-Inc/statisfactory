#! /usr/bin/python3

# pyproject_loader.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements utility functions to fetch the pyproject.toml
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from typing import Union
from pydantic import ValidationError
from pathlib import Path
import tomli

# project
from statisfactory.models.models import Pyproject
from statisfactory.errors import Errors

#############################################################################
#                                  Script                                   #
#############################################################################


def get_pyproject(path: Union[str, Path]) -> Pyproject:
    """
    Open and validate the statisfactory section of the pyproject.toml file
    """

    path = Path(path)

    # Extract the stati section from the pyproject
    try:
        with open(path, "rb") as f:
            pyproject_toml = tomli.load(f)
            config = pyproject_toml.get("tool", {}).get("statisfactory", {})
    except BaseException as error:
        raise Errors.E012(path=path) from error  # type: ignore

    # Validate the config
    try:
        config = Pyproject(**config)
    except (TypeError, ValidationError) as error:
        raise Errors.E011() from error  # type: ignore

    return config


