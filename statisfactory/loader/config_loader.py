#! /usr/bin/python3

# config_loader.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Exposes an utility function to load configurations yaml / jinja based configurations files for stati
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from typing import Any, Union, Dict, Mapping
from pathlib import Path
from functools import reduce
from copy import deepcopy

# project
from statisfactory.errors import Errors
from statisfactory.loader.yaml_utils import (
    gen_dictionaries_representation,
    recursive_merge_dict,
    merge_dict,
)

#############################################################################
#                                  Script                                   #
#############################################################################


def _expand_embedded_configs(*, name: str, configuration: Mapping, raw: Mapping):
    """
    Unnest the embedded mapping.
    """

    # Look for the special marker of nested configs : _from, and expand them
    config_mapping = []

    expand_item_name_list = configuration.get("_from")
    if expand_item_name_list:
        for expand_item_name in expand_item_name_list:

            expand_item_definition = raw.get(expand_item_name)
            if not expand_item_definition:
                raise Errors.E016(name=name, ref=expand_item_name)  # type: ignore

            config_mapping.append(
                _expand_embedded_configs(
                    name=expand_item_name,
                    configuration=expand_item_definition,
                    raw=raw,
                )
            )

    # Add the overwritting mapping
    for k, m in configuration.items():
        if k == "_from":
            continue
        config_mapping.append({k: deepcopy(m)})  # Copy to avoid mutating references

    # Process the config mapping and keep the last value
    config_mapping = reduce(lambda x, y: recursive_merge_dict(x, y), config_mapping)

    return config_mapping


def get_configurations(path: Union[str, Path], session) -> Dict[str, Any]:
    """
    Build the configuration definitions by recyrsively injecting pipelines definitions

    Returns:
        Dict[str, Any]: A mapping of pipelines names to `PipelinesDefinition`
    """

    render_vars = {k.lower(): v for k, v in session.settings.to_dict().items()}
    path = Path(path)

    # merge all the yamls
    mappers = gen_dictionaries_representation(path, render_vars)
    mapper = merge_dict(*mappers)  # type: ignore

    # Iterate over each configuration, and expand the definition
    loaded = {}
    for name, configuration in mapper.items():  # type: ignore
        config = _expand_embedded_configs(
            name=name, configuration=configuration, raw=mapper  # type: ignore
        )

        loaded[name] = config

    return loaded
