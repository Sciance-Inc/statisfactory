#! /usr/bin/python3
#
#    Statisfactory - A satisfying statistical factory
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
# yaml_utils.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements various helper functions to manipulated data extracted from yamls
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from typing import Any, Dict, Iterator, Mapping, Optional, Union, Generator, Tuple, List
from pathlib import Path
from glob import glob
import yaml
from functools import singledispatch

# Third party
from jinja2 import Template

# project
from statisfactory.errors import Errors
from statisfactory.models import PipelineDefinition, ParametersSetDefinition, Artifact

# Project type checks : see PEP563
# if TYPE_CHECKING:
#    from statisfactory.session import Session

#############################################################################
#                                  Script                                   #
#############################################################################

MODELS = Union[PipelineDefinition, ParametersSetDefinition, Artifact]


def gen_as_model(
    path: Path, model: MODELS, render_vars: Optional[Dict[str, Any]] = None
) -> Generator[Tuple[Optional[str], MODELS], None, None]:
    """
    Parse all the yamls files stored a 'path' and returr of generator of deserialized models
    Args:
        path (Path): the path to start parsing the yaml files for.
        model (MODELS): the model to deserialize the yaml files against.
        render_vars (Dict[str, Any]): an optional mapping of variables to use to render the templates. Default to None.
    Returns
        Iterator[Dict[str, Any]]: a tuple of parsed dictionaries. One for each one of the yaml found in 'path'
    """

    render_vars = render_vars or {}
    for template_path in _gen_yamls(Path(path)):
        rendered = _render_template(template_path, render_vars)
        templated = _load_template(rendered)

        if isinstance(templated, dict):
            for key, val in templated.items():
                yield key, model(**val)  # type: ignore
        elif isinstance(templated, list):
            for val in templated:
                yield None, model(**val)  # type: ignore


def _load_template(template: str) -> Union[List, Dict[str, Any]]:
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


def _render_template(path: Path, render_vars: Optional[Dict[str, Any]] = None) -> str:
    """
    Render the Jinja2 template from 'path' with  interpolated varaibles from 'render_vars'.

    Args:
        path (Path): the path to the ressource to render.
        render_vars (Dict[str, Any], optional): An optional mapping of variables to use to render the template. Defaults to None.

    Implementation details:
    * YAML parse nill and ~ as None.
    * YAML doesnt dump None as Null
    * The render_vars dict is interpolate from global, so any null or ~ has been replaced by a None
    * Before rendering, any None in the the render_vars dict replaced with a '~'
    * After rendering, and when parsed back, the None will be preserved
    """

    @singledispatch
    def replace_none(value):
        return value

    @replace_none.register(dict)
    def _(value):
        return {k: replace_none(v) for k, v in value.items()}

    @replace_none.register(list)
    def _(value):
        return [replace_none(v) for v in value]

    @replace_none.register(type(None))
    def _(value):
        return "null"

    # Load and render the Jinja template
    try:
        with open(path) as f:
            template = Template(f.read())
    except BaseException as error:
        raise Errors.E0182(path=str(path)) from error  # type: ignore

    # Replace the None from renders_vars with '~' because of the way yaml parse the None
    render_vars = replace_none(render_vars)

    try:
        rendered = template.render(render_vars)
    except BaseException as error:
        raise Errors.E0183(path=str(path), vars=render_vars)  # type: ignore

    return rendered


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


def override_merge_dict(L: Optional[Dict] = None, R: Optional[Dict] = None) -> Dict:
    """
    Merge the Right dictionary into the Left dictionary, overriding the keys alreayd defined in the Left dictionary.
    """

    L = L or {}
    R = R or {}

    return {**L, **R}


def recursive_merge_dict(L, R) -> Mapping:
    """
    Recursively merge R into L.
    The merge is recursive meaning that keys of two dictionnaries are not overrided but merged together.
    """
    for k in R.keys():
        if k in L and isinstance(L[k], dict) and isinstance(R[k], dict):
            recursive_merge_dict(L[k], R[k])
        else:
            L[k] = R[k]

    return L
