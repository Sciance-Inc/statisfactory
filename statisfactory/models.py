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
from dataclasses import dataclass, field
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

    connString: str
    name: str = None


class ConnectorSchema(Schema):
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


@dataclass
class Artefact:
    """
    Represents an Artefact : the I/O of a statisfactory node.
    """

    name: str
    type: str = None
    path: Optional[str] = None
    connector: Optional[Connector] = None
    query: Optional[str] = None
    save_options: Optional[Dict] = field(default_factory=dict)
    load_options: Optional[Dict] = field(default_factory=dict)

    def __post_init__(self):
        """
        Transform the marshmalling's list-of-dicts to a dict
        """

        self.save_options = _merge_dict(self.save_options)
        self.load_options = _merge_dict(self.load_options)


class ArtefactSchema(Schema):
    """
    Artefact's marshaller.
    """

    _valids_artefacts = ["odbc", "csv", "xslx", "datapane", "pickle", "binary"]

    # Shared
    type = fields.Str(required=True, validate=validate.OneOf(_valids_artefacts))
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
    def make_artefact(self, data, **kwargs):
        return Artefact(name=None, **data)


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

    version: str
    artefacts: List[Dict[str, Artefact]]
    connectors: List[Dict[str, Connector]] = field(default_factory=list)

    def __post_init__(self):
        """
        Transform the marshmalling's list-of-dicts to a dict
        """

        self.artefacts = _merge_dict(self.artefacts)
        self.connectors = _merge_dict(self.connectors)

        # Set the name attribute to each artefact
        for name, artefact in self.artefacts.items():
            artefact.name = name

        # Set the name attribute to each connector
        for name, conn in self.connectors.items():
            conn.name = name

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
            catalog = CatalogDataSchema().load(yaml.safe_load(f))

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
        fields.Mapping(fields.Str, fields.Nested(ConnectorSchema)), allow_none=True
    )

    @post_load
    def make_catalogData(self, data, **kwargs):
        return CatalogData(**data)


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    raise BaseException("model.py can't be run in standalone")
