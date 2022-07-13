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
from collections import defaultdict
from typing import Any, Union, Dict, Mapping, Iterable, Tuple
from pathlib import Path
from functools import reduce
from copy import deepcopy

# project
from statisfactory.models import ParametersSetDefinition, MergeMethod
from statisfactory.errors import Errors
from statisfactory.loader.yaml_utils import gen_as_model, override_merge_dict, recursive_merge_dict

#############################################################################
#                                  Script                                   #
#############################################################################

_merge_method_2_function = {MergeMethod.recursive_merge: recursive_merge_dict, MergeMethod.override: override_merge_dict}


def _merge_by_precedence(parameters_definitions: Iterable[Tuple[str, ParametersSetDefinition]]) -> Dict:
    """
    Merge the parameters set by precedence (the lower the precedence, the higher the priority )
    """

    # Build a dictionary of parameters sets, grouped by name
    tmp = defaultdict(list)
    for name, parameters in parameters_definitions:
        tmp[name].append(parameters)

    # Order each group by increasing precedence and take the first one
    out = {}
    for key in tmp.keys():
        first = sorted(tmp[key], key=lambda x: x.precedence)[0]
        out[key] = first

    return out


def _expand_embedded_configs(*, name: str, parameters: Mapping, raw: Mapping):
    """
    Unnest the embedded mapping.
    """

    # Look for the special marker of nested configs : _from, and expand them
    config_mapping = []

    tags = parameters["tags"]
    merge_method = _merge_method_2_function[parameters["merge"]]

    expand_item_name_list = parameters.get("from_")
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
        if k in set(ParametersSetDefinition.__fields__.keys()):
            continue
        config_mapping.append({k: deepcopy(m)})  # Copy to avoid mutating references

    # Process the config mapping and keep the last value
    config_mapping = reduce(lambda x, y: merge_method(x, y), config_mapping)

    # Assign the tags
    config_mapping["tags"] = tags

    return config_mapping


def get_parameters(path: Union[str, Path], session) -> Dict[str, Any]:
    """
    Build the parameters definitions by recyrsively injecting pipelines definitions
    """

    render_vars = {k.lower(): v for k, v in session.settings.to_dict().items()}
    path = Path(path)

    # merge all the yamls files, validate them as models and convert them to a dictionary
    mappers = gen_as_model(path=path, model=ParametersSetDefinition, render_vars=render_vars)  # type: ignore
    mapper = _merge_by_precedence(mappers)  # type: ignore
    mapper = {k: dict(v) for k, v in mapper.items()}

    # Iterate over each configuration, and expand the definition
    loaded = {}
    for name, parameters in mapper.items():  # type: ignore
        config = _expand_embedded_configs(name=name, parameters=parameters, raw=mapper)  # type: ignore
        loaded[name] = config

    return loaded
