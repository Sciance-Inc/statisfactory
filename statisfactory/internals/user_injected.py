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
# maker.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements a metaclass to inject user defined object to replace the default of stati.
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

import sys
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Type
from importlib import import_module
from pathlib import Path
from statisfactory.loader import (
    get_path_to_target,
    get_pyproject,
)

#############################################################################
#                                  Script                                   #
#############################################################################


@contextmanager
def _add_path(path: str):
    """
    CM. Adds a path to the path envir
    """

    sys.path.insert(0, path)
    yield
    sys.path.pop(0)


@dataclass
class Prototype:
    factory_name: str
    default_factory: Type


class UserInjected(type):
    """
    A metaclass to dynamically select the appropriate bases for the Session object.
    The custom base to inject inplace of the statisfactory.session.BaseSession can be set through the pyproject.toml file
    """

    _cls_to_protype = dict()

    def __new__(cls, name, bases, namespace, **kwargs):

        prototype = kwargs.get("prototype", None)
        is_registered = UserInjected._cls_to_protype.get(name, None)

        if not is_registered:
            if not prototype:
                raise RuntimeError("The UserInjected metaclass expects an entrypoints arguments")

            UserInjected._cls_to_protype[name] = prototype

        return super().__new__(cls, name, bases, namespace)

    def __call__(cls, root_folder=None):
        """
        Return a new Session class type to be used to instanciate Sessions.
        The new class inherits from the user defined one (if provide.)
        """

        prototype = UserInjected._cls_to_protype[cls.__name__]

        # Retrieve the location of the config file
        root = Path(root_folder or get_path_to_target("pyproject.toml"))

        # If no custom factory is defined, then return the base session.
        config = get_pyproject(root / "pyproject.toml")

        factory = prototype.default_factory
        if config.entrypoints:

            # Always import the module to get the potential sides effects.
            module = config.entrypoints.module  # type: ignore
            with _add_path(str(root)):
                mod = import_module(module)
                # Potentialy import the factory
                obj = getattr(config.entrypoints, prototype.factory_name)
                if obj:
                    factory = getattr(mod, obj)

        # Create a new class inheriting from the factory
        session_class = type(cls.__name__, (cls, factory), {})  # type: ignore
        return super(UserInjected, session_class).__call__(root_folder=root_folder)  # type: ignore
