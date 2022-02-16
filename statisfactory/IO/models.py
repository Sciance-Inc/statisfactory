#! /usr/bin/python3

# main.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    implements models serialization for yaml IO files.
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

from dataclasses import dataclass, field

# system
from typing import Dict, List, Optional
from warnings import warn

import yaml
from marshmallow import Schema, ValidationError, fields, post_load, validates_schema

# third party
from statisfactory.errors import Warnings

# project


#############################################################################
#                                  Script                                   #
#############################################################################


@dataclass
class Connector:
    """
    Represents an odbc connection.
    """

    connString: str
    name: str = None


class _ConnectorSchema(Schema):
    """
    Connector's marshmaller
    """

    connString = fields.Str()

    @post_load
    def make_connector(self, data, **kwargs):
        return Connector(**data)


# ------------------------------------------------------------------------- #


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


@dataclass
class Artifact:
    """
    Represents an Artifact : the I/O of a statisfactory node.
    """

    name: str
    type: str = ""
    path: Optional[str] = None
    description: Optional[str] = ""
    connector: Optional[Connector] = None
    query: Optional[str] = None
    save_options: Optional[Dict] = field(default_factory=dict)
    load_options: Optional[Dict] = field(default_factory=dict)

    @staticmethod
    def of(*args) -> List["Artifact"]:
        """
        Convenient helper to return a tuple of Artifact from an iterable of strings.
        """

        return [Artifact(i) for i in args]

    def __post_init__(self):
        """
        Transform the marshmalling's list-of-dicts to a dict
        """

        self.save_options = _merge_dict(self.save_options)
        self.load_options = _merge_dict(self.load_options)


class _ArtifactSchema(Schema):
    """
    Artifact's marshaller.
    """

    valids_artifacts = set()

    # Shared
    type = fields.Str(required=True)
    abstract = fields.Bool(required=False)

    # file
    path = fields.Str()

    # Sql
    query = fields.Str()
    connector = fields.Str()

    # Options
    save_options = fields.List(fields.Mapping(keys=fields.Str, required=False))
    load_options = fields.List(fields.Mapping(keys=fields.Str, required=False))

    @validates_schema
    def validate_file(self, data, **kwargs):
        errors = {}
        if data["type"] in ["csv", "xslx"]:
            if "path" not in data:
                errors["path"] = ["missing path for csvdataset"]

        if errors:
            raise ValidationError(errors)

    @validates_schema
    def validate_query(self, data, **kwargs):
        errors = {}
        if data["type"] == "odbc":
            if "connector" not in data:
                errors["odbc"] = ["missing connector for type odbc"]
            if "query" not in data:
                errors["odbc"] = ["missing query for type odbc"]

        if errors:
            raise ValidationError(errors)

    @validates_schema
    def validate_datapane(self, data, **kwargs):
        errors = {}
        if data["type"] == "datapane":
            if "path" not in data:
                errors["datapane"] = ["missing path for type datapane"]

        if errors:
            raise ValidationError(errors)

    @post_load
    def make_artifact(self, data, **kwargs):
        if data["type"] not in _ArtifactSchema.valids_artifacts:
            warn(Warnings.W020.format(inter_type=data["type"]))  # type: ignore

        return Artifact(name=None, **data)


# ------------------------------------------------------------------------- #


def _merge_dict(dicts: List[Dict]) -> Dict:
    """
    Merge a list of dict in a python 3.5 comptible way
    """
    if not dicts:
        return {}

    out = {}
    for dict in dicts:
        out.update(dict)
    return out


@dataclass
class CatalogData:
    """
    State of the catalogue.
    """

    artifacts: Dict[str, Artifact]
    connectors: Dict[str, Connector]  # = field(default_factory=list)

    def __post_init__(self):
        """
        Transform the marshmalling's list-of-dicts to a dict
        """

        self.artifacts = _merge_dict(self.artifacts)
        self.connectors = _merge_dict(self.connectors)

        # Set the name attribute to each artifact
        for name, artifact in self.artifacts.items():
            artifact.name = name

        # Set the name attribute to each connector
        for name, conn in self.connectors.items():
            conn.name = name

    @staticmethod
    def from_string(s: str) -> "CatalogData":
        """
        Build a catalog data from a string
        """

        return _CatalogDataSchema().load(yaml.safe_load(s))


class _CatalogDataSchema(Schema):
    """
    CatalogueData's marshaller.
    """

    artifacts = fields.List(
        fields.Mapping(fields.Str, fields.Nested(_ArtifactSchema)), required=True
    )

    connectors = fields.List(
        fields.Mapping(fields.Str, fields.Nested(_ConnectorSchema)), allow_none=True
    )

    @post_load
    def make_catalogData(self, data, **kwargs):
        return CatalogData(**data)


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    raise BaseException("model.py can't be run in standalone")
