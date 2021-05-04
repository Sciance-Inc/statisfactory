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

import dataclasses

# system
from typing import Any, List, Mapping, Optional

import yaml

# third party
from marshmallow import Schema, fields, post_load

# project
from ...errors import errors

#############################################################################
#                                  Script                                   #
#############################################################################


@dataclasses.dataclass
class PipelineDefinition:
    """
    Represents the definition of a Pipeline
    """

    operators: List[str]
    config: Optional[Mapping[str, Any]] = dataclasses.field(default_factory=dict)

    @staticmethod
    def from_file(file: str):
        """
        Build a statisfactory config from a file.
        """

        try:
            with open(file) as f:
                data = yaml.safe_load(f)
        except BaseException as error:
            raise errors.E012(
                __name__, file="Pipelines definition", path=file
            ) from error

        schema = Schema.from_dict(
            {key: fields.Nested(_PipelinesDefinitionsShema) for key in data}
        )

        try:
            return schema().load(data)
        except BaseException as error:
            raise errors.E013(__name__, file="Pipelines definition") from error


class _PipelinesDefinitionsShema(Schema):

    operators = fields.List(fields.Str(), required=True)
    config = fields.Mapping(keys=fields.Str(), required=False)

    @post_load
    def make_PipelinesDefinitions(self, data, **kwargs):
        return PipelineDefinition(**data)


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    raise BaseException("model.py can't be run in standalone")
