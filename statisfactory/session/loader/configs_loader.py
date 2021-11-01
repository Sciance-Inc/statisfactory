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
from ...errors import Errors

#############################################################################
#                                  Script                                   #
#############################################################################


class ConfigsLoader:
    """
    Namespace for parsing the config file and build the configs mapping.
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
                and isinstance(R[k], collections.Mapping)  # type: ignore
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
                    raise Errors.E016(name=name, ref=expand_item_name)  # type: ignore

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
            raise Errors.E012(file="Pipelines configuration", path=path)  # type: ignore

        # Iterate over each configuration, and expend the definition
        loaded = {}
        for name, configuration in m.items():  # type: ignore
            config = ConfigsLoader._expand_embedded_configs(
                name=name, configuration=configuration, raw=m  # type: ignore
            )

            # The Pipeline.__call__ expects a "namespaced" mapping and a variadic keyword mapping for shared variables : I rewrite the parsed configuation
            pipeline_config = config.get("_shared", {})
            pipeline_config["namespaced"] = {
                key: val for key, val in config.items() if key != "_shared"
            }
            loaded[name] = pipeline_config

        return loaded


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    raise BaseException("pipeline_parser.py can't be run in standalone")
