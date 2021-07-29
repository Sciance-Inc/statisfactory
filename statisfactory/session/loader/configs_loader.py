#! /usr/bin/python3

# configs_loader.py
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
import collections
from functools import reduce
from typing import Any, Mapping

# Third party
import anyconfig

# project
# from ...operator import Pipeline
from ...errors import errors

#############################################################################
#                                  Script                                   #
#############################################################################


class ConfigsLoader:
    """
    Namespace for to parse the config file and build the configs mapping.
    """

    @staticmethod
    def _dict_merge(L, R) -> Mapping:
        """
        Recuirsively merge R into L.
        """
        for k, v in R.items():
            if (
                k in L
                and isinstance(L[k], dict)
                and isinstance(R[k], collections.Mapping)
            ):
                ConfigsLoader._dict_merge(L[k], R[k])
            else:
                L[k] = R[k]

        return L

    @staticmethod
    def _expand_embedded_configs(*, name: str, configuration: Mapping, raw: Mapping):
        """
        Unnest the embedded mapping.s
        """

        # Look for the special marker of nested configs : _from, and expand them
        config_mapping = []

        expand_item_name_list = configuration.get("_from")
        if expand_item_name_list:
            for expand_item_name in expand_item_name_list:

                expand_item_definition = raw.get(expand_item_name)
                if not expand_item_definition:
                    raise errors.E016(__name__, name=name, ref=expand_item_name)

                config_mapping.append(
                    ConfigsLoader._expand_embedded_configs(
                        name=expand_item_name,
                        configuration=expand_item_definition,
                        raw=raw,
                    )
                )

        # Add the overwritting mapping
        for k, m in configuration.items():
            if k == "_from":
                continue
            config_mapping.append({k: m.copy()})  # Copy to avoid mutating references

        # Process the config mapping and keep the last value
        config_mapping = reduce(
            lambda x, y: ConfigsLoader._dict_merge(x, y), config_mapping
        )

        return config_mapping

    @staticmethod
    def load(*, path: str) -> Mapping[str, Any]:
        """
        Build the pipelines by parsing file located in `path`.
        The config file support embedding of definitions. One can declare a top-level
        configuration and use it in other definition using the _from syntax
        """

        # Parse the YAML
        try:
            m = anyconfig.load(path)
        except FileNotFoundError:
            raise errors.E012(__name__, file="Pipelines configuration", path=path)

        # Iterate over each configuration, and expend the definition
        loaded = {}
        for name, configuration in m.items():
            loaded[name] = ConfigsLoader._expand_embedded_configs(
                name=name, configuration=configuration, raw=m
            )

        return loaded


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    raise BaseException("pipeline_parser.py can't be run in standalone")
