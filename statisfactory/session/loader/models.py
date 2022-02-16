#! /usr/bin/python3

# models.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    implements models serialization for Pipelines definitions and Configuration files
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from typing import Any, List, Mapping, Optional
import dataclasses
from pydantic.dataclasses import dataclass

# project
from statisfactory.errors import Errors

#############################################################################
#                                  Script                                   #
#############################################################################


@dataclass
class PipelineDefinition:
    """
    Represents the definition of a Pipeline
    """

    class Config:
        extra = "forbid"

    operators: List[str]


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    raise BaseException("model.py can't be run in standalone")
