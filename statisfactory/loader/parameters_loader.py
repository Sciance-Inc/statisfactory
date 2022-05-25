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


def _expand_embedded_configs(*, name: str, parameters: Mapping, raw: Mapping):
    """
    Unnest the embedded mapping.
    """

    # Look for the special marker of nested configs : _from, and expand them
    config_mapping = []

    expand_item_name_list = parameters.get("_from")
    if expand_item_name_list:
        for expand_item_name in expand_item_name_list:

            expand_item_definition = raw.get(expand_item_name)
            if not expand_item_definition:
                raise Errors.E016(name=name, ref=expand_item_name)  # type: ignore

            config_mapping.append(
                _expand_embedded_configs(
                    name=expand_item_name,
                    parameters=expand_item_definition,
                    raw=raw,
                )
            )

    # Add the overwritting mapping
    for k, m in parameters.items():
        if k == "_from":
            continue
        config_mapping.append({k: deepcopy(m)})  # Copy to avoid mutating references

    # Process the config mapping and keep the last value
    config_mapping = reduce(lambda x, y: recursive_merge_dict(x, y), config_mapping)

    return config_mapping


def get_parameters(path: Union[str, Path], session) -> Dict[str, Any]:
    """
    Build the parameters definitions by recyrsively injecting pipelines definitions
    """

    render_vars = {k.lower(): v for k, v in session.settings.to_dict().items()}
    path = Path(path)

    # merge all the yamls
    mappers = gen_dictionaries_representation(path, render_vars)
    mapper = merge_dict(*mappers)  # type: ignore

    # Iterate over each configuration, and expand the definition
    loaded = {}
    for name, parameters in mapper.items():  # type: ignore
        config = _expand_embedded_configs(
            name=name, parameters=parameters, raw=mapper  # type: ignore
        )

        loaded[name] = config

    return loaded
