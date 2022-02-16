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
import collections
from importlib import import_module
from typing import Any, Union, Dict, Iterator, Tuple, TYPE_CHECKING, Mapping
from pathlib import Path
from glob import glob
import yaml
from abc import ABCMeta, abstractmethod
from functools import reduce
from copy import deepcopy

# Third party
from jinja2 import Template

# project
from statisfactory.errors import Errors
from statisfactory.session.loader.models import PipelineDefinition
from statisfactory.operator import Pipeline

# Project type checks : see PEP563
# if TYPE_CHECKING:
#    from statisfactory.session import Session

#############################################################################
#                                  Script                                   #
#############################################################################


class _ConfigLoader(metaclass=ABCMeta):
    """
    Primitives to parse, render and build the configurations files used by Stati.
    Stati config files are YAML files with jinja
    """

    def __init__(self, path: Union[str, Path], session):
        """
        Instanciates a new config loader

        Args:
            path (Union[Path, str]): the path to start parsing the yaml files for.
            session (Session): the statisfactory.Session object to use to extract variables tro be interpolated from.
        """

        # Extract the variables to be injected in the template from the session
        self._render_vars = {
            k.lower(): v for k, v in session.settings.to_dict().items()  # type: ignore
        }

        self._path = Path(path)

    @property
    @abstractmethod
    def data(self) -> Dict[str, Any]:
        """
        Getter for the parsed config
        """

        raise NotImplementedError("Must be implemented in the concrete deserializer.")

    @staticmethod
    def gen_dictionaries_representation(
        path: Path, render_vars: Dict[str, Any] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Parse all the yamls files stored a 'path'.

        Args:
            path (Path): the path to start parsing the yaml files for.
            render_vars (Dict[str, Any]): an optional mapping of variables to use to render the templates. Default to None.

        Returns
            Iterator[Dict[str, Any]]: a tuple of parsed dictionaries. One for each one of the yaml found in 'path'
        """

        render_vars = render_vars or {}

        for template_path in _ConfigLoader._gen_yamls(Path(path)):
            rendered = _ConfigLoader._render_template(template_path, render_vars)
            mapper = _ConfigLoader._load_template(rendered)
            yield mapper

    @staticmethod
    def _load_template(template: str) -> Dict[str, Any]:
        """
        Load template and return dictionary representation fo the yaml.

        Args:
            template (str): a string to be loaded as yaml into a dictionary.

        Returns:
            Dict[str, Any]: a mapping of parsed values
        """

        try:
            parsed = yaml.safe_load(template)  # type: ignore
        except BaseException as error:
            raise Errors.E0184(repr=template) from error  # type: ignore

        return parsed

    @staticmethod
    def _render_template(path: Path, render_vars: Dict[str, Any] = None) -> str:
        """
        Render the Jinja2 template from 'path' with  interpolated varaibles from 'render_vars'.

        Args:
            path (Path): the path to the ressource to render.
            render_vars (Dict[str, Any], optional): An optional mapping of variables to use to render the template. Defaults to None.
        """

        # Load and render the Jinja template
        try:
            with open(path) as f:
                template = Template(f.read())
        except BaseException as error:
            raise Errors.E0182(path=str(path)) from error  # type: ignore

        try:
            rendered = template.render(render_vars)
        except BaseException as error:
            raise Errors.E0183(path=str(path), vars=render_vars)  # type: ignore

        return rendered

    @staticmethod
    def _gen_yamls(path: Path) -> Iterator[Path]:
        """
        Iterates over all the yamls found at the 'path' location.
        If 'path' is a yaml, only the yaml is returned.
        If 'path' is a folder, any yaml in the folder will be returned.

        Args:
            path (Path): the source folder we want to extracts yaml from.
        """

        # make sure the path actually exists
        if not path.exists():
            raise Errors.E0181(path=str(path))  # type: ignore

        # If the path is the file, then return a generator of one
        if path.is_file():
            yield path
        else:
            for files in (path / "**/*.yml", path / "**/*.yaml"):
                for item in (Path(g) for g in glob(str(files), recursive=True)):
                    yield item

        return None


def _merge_dict(*dicts: Iterator[Dict], collide=True) -> Dict:
    """
    Merge a list of dict in a python 3.5 comptible way
    """

    if not dicts:
        return {}

    out = {}
    for dict_ in dicts:
        out.update(dict_)  # type: ignore
    return out


def _recursive_merge_dict(L, R) -> Mapping:
    """
    Recursively merge R into L.
    The merge is recursive meaning that keys of two dictionnaries are not overrided but merged together.
    """
    for k, v in R.items():
        if k in L and isinstance(L[k], dict) and isinstance(R[k], collections.Mapping):
            _recursive_merge_dict(L[k], R[k])
        else:
            L[k] = R[k]

    return L


class ConfigurationsLoader(_ConfigLoader):
    """
    Load the Configurations definitions

    Args:
        path (Union[Path, str]): the path to start parsing the yaml files for.
        session (Session): the statisfactory.Session object to use to extract variables tro be interpolated from.
    """

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
                    ConfigurationsLoader._expand_embedded_configs(
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
        config_mapping = reduce(
            lambda x, y: _recursive_merge_dict(x, y), config_mapping
        )

        return config_mapping

    @property
    def data(self) -> Dict[str, Any]:
        """
        Build the configuration definitions by recyrsively injecting pipelines definitions

        Returns:
            Dict[str, Any]: A mapping of pipelines names to `PipelinesDefinition`
        """

        # merge all the yamls
        mappers = _ConfigLoader.gen_dictionaries_representation(
            self._path, self._render_vars
        )
        mapper = _merge_dict(*mappers)  # type: ignore

        # Iterate over each configuration, and expand the definition
        loaded = {}
        for name, configuration in mapper.items():  # type: ignore
            config = ConfigurationsLoader._expand_embedded_configs(
                name=name, configuration=configuration, raw=mapper  # type: ignore
            )

            loaded[name] = config

        return loaded


class PipelinesLoader(_ConfigLoader):
    """
    Load the pipelines
    """

    @staticmethod
    def _load_pipeline(name, definition: Mapping, raw: Mapping) -> Pipeline:

        P = Pipeline(name=name)  # By default, YAML pipelines are namespaced
        for target_name in definition.operators:  # type: ignore

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

    @property
    def data(self) -> Dict[str, Any]:
        """
        Build the pipelines representation by recyrsively injecting pipelines definitions

        Returns:
            Dict[str, Any]: A mapping of pipelines names to `PipelinesDefinition`
        """

        # merge all the yamls
        mappers = _ConfigLoader.gen_dictionaries_representation(
            self._path, self._render_vars
        )
        mapper = _merge_dict(*mappers)  # type: ignore

        # Deserialize against the model
        mapper = {k: PipelineDefinition(**v) for k, v in mapper.items()}

        # Iterate over each Definition, and create the a Pipeline object.
        loaded = {}
        for name, definition in mapper.items():
            loaded[name] = PipelinesLoader._load_pipeline(name, definition, mapper)

        return loaded
