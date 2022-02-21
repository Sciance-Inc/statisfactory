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
from typing import List, Dict, Optional, Any
from pathlib import Path
from pydantic import Field, BaseModel
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
        # pipeline_factory: Optional[str]
        # craft_factory: Optional[str]

    project_slug: str
    configuration: str
    catalog: str
    sources: Optional[Path]
    notebook_target: Optional[Path]
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


@dataclass
class PipelineDefinition:
    """
    Represents the definition of a Pipeline
    """

    operators: List[str]


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
