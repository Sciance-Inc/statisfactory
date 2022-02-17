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
from pydantic import BaseModel
from pydantic import Extra
from pydantic.dataclasses import dataclass


#############################################################################
#                                  Script                                   #
#############################################################################


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
class Artifact(BaseModel, extra=Extra.allow):
    """
    Represents an Artifact : the I/O of a statisfactory node.
    """

    name: Optional[str]
    save_options: Optional[Dict[str, Any]] = {}
    load_options: Optional[Dict[str, Any]] = {}
    type: str = ""

    @staticmethod
    def of(*args) -> List["Artifact"]:
        """
        Convenient helper to return a tuple of Artifact from an iterable of strings.
        """
        _ = lambda name: Artifact(name=name, save_options={}, load_options={})
        return [_(i) for i in args]


@dataclass
class Connector:
    """
    Represents a connector used by the SQL Artifact
    """

    connString: str
    name: str = ""


@dataclass
class CatalogData:

    artifacts: Dict[str, Artifact]
    connectors: Optional[Dict[str, Connector]]

    def __post_init_post_parse__(self):
        """
        Assign the artifact / connector name to their respective object.
        """

        # Set the name attribute to each artifact
        for name, artifact in self.artifacts.items():
            artifact.name = name  # type: ignore

        # Set the name attribute to each connector
        self.connectors = self.connectors or {}
        for name, conn in self.connectors.items():
            conn.name = name  # type: ignore


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    raise BaseException("model.py can't be run in standalone")
