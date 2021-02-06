#! /usr/bin/python3

# main.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    implements models serialization for yaml config files.
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from typing import List, Dict, Optional
import yaml

# project

# third party
from dataclasses import dataclass
from marshmallow import (
    Schema,
    fields,
    validates_schema,
    ValidationError,
    validate,
    post_load,
)


#############################################################################
#                                  Script                                   #
#############################################################################


@dataclass
class Connector:
    """
    Represents an odbc connection.
    """

    connectionString: str


class ConnectorSchema(Schema):
    """
    Connector's marshmaller
    """

    connectionString = fields.Str()

    @post_load
    def make_connector(self, data, **kwargs):
        return Connector(**data)


# ------------------------------------------------------------------------- #


@dataclass
class Artefact:
    """
    Represents an Artefact : the I/O of a statisfactory node.
    """

    type: str
    path: Optional[str] = None
    query: Optional[str] = None
    connector: Optional[Connector] = None


class ArtefactSchema(Schema):
    """
    Artefact's marshaller.
    """

    # Shared
    type = fields.Str(required=True, validate=validate.OneOf(["odbc", "csv", "xslx"]))
    abstract = fields.Bool(required=False)

    # file
    path = fields.Str()

    # Sql
    query = fields.Str()
    connector = fields.Str()

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
            if "query" not in data:
                errors["odbc"] = ["missing query for type odbc"]
            if "connector" not in data:
                errors["odbc"] = ["missing connector for type odbc"]

        if errors:
            raise ValidationError(errors)

    @post_load
    def make_artefact(self, data, **kwargs):
        return Artefact(**data)


# ------------------------------------------------------------------------- #


def _merge_dict(dicts: List[Dict]) -> Dict:
    """
    Merge a list of dict in a python 3.5 comptible way
    """
    out = {}
    for dict in dicts:
        out.update(dict)
    return out


@dataclass
class CatalogData:
    """
    State of the catalogue.
    """

    version: str
    artefacts: List[Dict[str, Artefact]]
    connectors: List[Dict[str, Connector]]

    def __post_init__(self):
        """
        Transform the marshmalling's list-of-dicts to a dict

        python 3.5
        """

        self.artefacts = _merge_dict(self.artefacts)
        self.connectors = _merge_dict(self.connectors)

    @staticmethod
    def from_file(file: str) -> "CatalogData":
        """
        Build a catalog from a dictionary dump.

        Args:
            data (Dict): the dictionary representation of the data

        Returns:
            CatalogData: a catalogue to push and retriece data from.
        """

        with open(file) as f:
            catalog = CatalogDataSchema().load(yaml.load(f))

        return catalog


class CatalogDataSchema(Schema):
    """
    CatalogueData's marshaller.
    """

    version = fields.String(required=True)
    artefacts = fields.List(
        fields.Mapping(fields.Str, fields.Nested(ArtefactSchema)), required=True
    )

    connectors = fields.List(
        fields.Mapping(fields.Str, fields.Nested(ConnectorSchema)), required=True
    )

    @post_load
    def make_catalogData(self, data, **kwargs):
        return CatalogData(**data)


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    raise BaseException("model.py can't be run in standalone")
