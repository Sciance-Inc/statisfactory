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
# parser.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements base classe in charge of the parsing of the various configurations / pipelines definitions file
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from importlib import import_module
from typing import Union, Dict, Mapping
from pathlib import Path

# project
from statisfactory.errors import Errors
from statisfactory.operator import Pipeline
from statisfactory.models.models import PipelineDefinition
from statisfactory.loader.yaml_utils import (
    gen_dictionaries_representation,
    merge_dict,
)


#############################################################################
#                                  Script                                   #
#############################################################################


def _load_pipeline(name, definition: PipelineDefinition, raw: Mapping) -> Pipeline:

    P = Pipeline(name=name)  # By default, YAML pipelines are namespaced
    for target_name in definition.operators:  # type: ignore

        # If the name is declared in raw -> then it's a pipeline to be built
        is_pipeline = target_name in raw
        if is_pipeline:
            P = P + _load_pipeline(target_name, raw[target_name], raw)

        # If not, then it's a Craft to be imported
        else:
            callable_, *modules = target_name.split(".")[::-1]
            modules = modules[::-1]
            try:

                craft = getattr(
                    import_module(".".join(modules)),
                    callable_,
                )
            except ImportError as error:
                raise Errors.E015(
                    pip_name=name, module=".".join(modules)
                ) from error  # type: ignore
            except AttributeError as error:
                raise Errors.E017(
                    pip_name=name,
                    module=".".join(modules),
                    craft_name=callable_,
                ) from error  # type: ignore

            P = P + craft

    return P


def get_pipelines(path: Union[str, Path], session) -> Dict[str, Pipeline]:

    """
    Build the pipelines representation by recyrsively injecting pipelines definitions

    Returns:
        Dict[str, Any]: A mapping of pipelines names to `PipelinesDefinition`
    """

    render_vars = {k.lower(): v for k, v in session.settings.to_dict().items()}
    path = Path(path)

    # merge all the yamls
    mappers = gen_dictionaries_representation(path, render_vars)
    mapper = merge_dict(*mappers)  # type: ignore

    # Deserialize against the model
    mapper = {k: PipelineDefinition(**v) for k, v in mapper.items()}

    # Iterate over each Definition, and create the a Pipeline object.
    loaded = {}
    for name, definition in mapper.items():
        loaded[name] = _load_pipeline(name, definition, mapper)

    return loaded
