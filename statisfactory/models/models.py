#! /usr/bin/python3
#
#    Statisfactory - A satisfying statistical facotry
#    Copyright (C) 2021-2022  Hugo Juhel
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
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
from typing import List, Dict, Optional, Any
from enum import Enum
from pathlib import Path
from pydantic import Field, BaseModel, Extra
from pydantic.dataclasses import dataclass

#############################################################################
#                                  Script                                   #
#############################################################################


class Pyproject(BaseModel):
    """
    The parsed configurations from the project.toml
    """

    # @dataclass
    class Entrypoints(BaseModel):
        module: str
        session_factory: Optional[str]

    project_slug: str
    configuration: str
    catalog: str
    sources: Optional[Path] = Path("lib")
    notebook_target: Optional[Path] = Path("jupyter")
    notebook_sources: Optional[Path] = Path("scripts")
    parameters: Optional[Path]
    pipelines_definitions: Optional[Path]
    entrypoints: Optional[Entrypoints]

    def dict(self):
        """
        Return a dictionary of non None fields

        Returns:
            _type_: _description_
        """
        return {k: v for k, v in self.__dict__.items() if v is not None}


class PipelineDefinition(BaseModel):
    """
    Represents the definition of a Pipeline
    """

    tags: Optional[List[str]] = Field(default_factory=list, alias="+tags")

    # A list of crafts / pipelines to be added to this pipeline
    operators: List[str] = Field(alias="+operators")


class MergeMethod(Enum):
    """
    Enum for the merge method
    """

    # "Their value with be replaced with our value"
    override = "override"
    # Recursive merge the dictionaries
    recursive_merge = "recursive_merge"


class ParametersSetDefinition(BaseModel, extra=Extra.allow):
    """
    Represent a parsed parameter set
    """

    tags: Optional[List[str]] = Field(default_factory=list, alias="+tags")

    # Other parameters set to inherits from
    from_: Optional[List[str]] = Field(default_factory=list, alias="+from")

    # The precedence order for calling parameters sets name
    precedence: Optional[int] = Field(default=10, alias="+precedence")

    # Key mergin mechanism
    # How to merge parameters of multiples priority
    merge: Optional[MergeMethod] = Field(default=MergeMethod.recursive_merge, alias="+merge")


@dataclass
class Volatile:
    """
    Represents data outputed by the Craft but not to be persisted in the catalog
    """

    name: str

    @staticmethod
    def of(*args) -> List["Volatile"]:
        """
        Convenient helper to return a tuple of volatile from an iterable of strings.
        """

        return [Volatile(i) for i in args]


# Extra.allow does not work wit dataclasses
@dataclass
class Artifact:
    """
    Represents an Artifact : the I/O of a statisfactory node.
    """

    name: str
    extra: Optional[Dict[str, Any]] = Field(default_factory=lambda: dict())
    save_options: Optional[Dict[str, Any]] = Field(default_factory=lambda: dict())
    load_options: Optional[Dict[str, Any]] = Field(default_factory=lambda: dict())
    type: str = ""

    @staticmethod
    def of(*args) -> List["Artifact"]:
        """
        Convenient helper to return a tuple of Artifact from an iterable of strings.
        """
        _ = lambda name: Artifact(name=name, save_options={}, load_options={}, extra={})
        return [_(i) for i in args]


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    raise BaseException("model.py can't be run in standalone")
