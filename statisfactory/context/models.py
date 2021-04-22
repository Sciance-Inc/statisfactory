#! /usr/bin/python3

# models.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    implements models serialization for Statisfactory Configuration files
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from typing import Optional
from dataclasses import dataclass
import yaml

# project
from ..errors import errors

# third party
from marshmallow import (
    Schema,
    fields,
    post_load,
)


#############################################################################
#                                  Script                                   #
#############################################################################


@dataclass
class StatisfactoryConfig:
    """
    Represents the configuration parsed from a statisfactory file
    """

    configuration: str
    catalog: str
    sources: Optional[str] = None
    pipelines_definitions: Optional[str] = None

    @staticmethod
    def from_file(file: str) -> "StatisfactoryConfig":
        """
        Build a statisfactory config from a file.
        """

        try:
            with open(file) as f:
                config = _StatisfactorySchema().load(yaml.safe_load(f))
        except BaseException as error:
            raise errors.logger_name(
                __name__, file="Statisfactory config", path=file
            ) from error

        return config


class _StatisfactorySchema(Schema):
    """
    Statisfactory Config's marshaller.
    """

    configuration = fields.Str(required=True)
    catalog = fields.Str(required=True)
    sources = fields.Str()
    pipelines_definitions = fields.Str()

    @post_load
    def make_StatisfactoryConfig(self, data, **kwargs):
        return StatisfactoryConfig(**data)


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    raise BaseException("model.py can't be run in standalone")
