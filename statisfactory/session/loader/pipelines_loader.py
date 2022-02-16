#! /usr/bin/python3

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
from statisfactory.session.loader.models import PipelineDefinition
from statisfactory.session.loader.yaml_utils import (
    gen_dictionaries_representation,
    merge_dict,
)

# Project type checks : see PEP563
# if TYPE_CHECKING:
#    from statisfactory.session import Session

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
