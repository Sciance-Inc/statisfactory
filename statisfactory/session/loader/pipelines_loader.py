#! /usr/bin/python3

# pipelines_loader.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements a parser for the Pipelines definitions
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from typing import Mapping
from importlib import import_module

# project
from .models import PipelineDefinition
from ...operator import Pipeline
from ...errors import errors


#############################################################################
#                                  Script                                   #
#############################################################################


class PipelinesLoader:
    """
    Parse the Pipelines and definition files and and return operator.Pipeline objects.
    """

    @staticmethod
    def _load_pipeline(name, definition: Mapping, raw: Mapping) -> Pipeline:

        P = Pipeline(name=name, **definition.config)
        for target_name in definition.operators:

            # If the name is declared in raw -> then it's a pipeline to be built
            is_pipeline = target_name in raw
            if is_pipeline:
                P = P + PipelinesLoader._load_pipeline(
                    target_name, raw[target_name], raw
                )

            # If not, then it's a Craft to be imported
            else:
                callable_, *modules = target_name.split(".")[::-1]
                modules = modules[::-1]
                try:
                    craft = getattr(
                        import_module("__compiled__." + ".".join(modules)), callable_
                    )
                except ImportError as error:
                    raise errors.E015(
                        __name__, pip_name=name, craft_name=target_name
                    ) from error

                P = P + craft

        return P

    @staticmethod
    def load(*, path: str) -> Mapping[str, Pipeline]:
        """
        Build the pipelines by parsing file located in `path`.
        """

        # Parse the YAML
        m = PipelineDefinition.from_file(path)

        # Iterate over each Definition, and create the a Pipeline object.
        loaded = {}
        for name, definition in m.items():
            loaded[name] = PipelinesLoader._load_pipeline(name, definition, m)

        return loaded


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    raise BaseException("pipeline_parser.py can't be run in standalone")
