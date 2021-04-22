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
from typing import Optional, List, Mapping
from dataclasses import dataclass
import yaml

# project
from ...errors import errors

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
class PipelineDefinition:
    """
    Represents the definition of a Pipeline
    """

    crafts: List[str]
    conf: Mapping


class PipelineDefinitionShema(Schema):

    crafts = fields.List(fields.Str(), required=True)
    # conf = fields.Mapping(keys=fields.Str(), required=False)


class PipelinesDefinitions(Schema):
    """
    PipelineDefinition's marshaller.
    """

    @staticmethod
    def from_file(file: str):
        """
        Build a statisfactory config from a file.
        """

        # try:

        with open(file) as f:
            data = yaml.safe_load(f)

        schema = PipelinesDefinitions.from_dict(
            {key: fields.Nested(PipelineDefinitionShema) for key in data}
        )

        return schema().load(data)

        # except BaseException as error:
        #    raise errors.E012(__name__, path=file) from error

        return config


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    raise BaseException("model.py can't be run in standalone")
