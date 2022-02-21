#! /usr/bin/python3

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
                raise RuntimeError("The MetaMaker expects an entrypoints arguments")

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
            mod = import_module(module)

            # Potentialy import the factory
            obj = getattr(config.entrypoints, prototype.factory_name)
            if obj:
                factory = getattr(mod, obj)

        # Create a new class inheriting from the factory
        session_class = type(cls.__name__, (cls, factory), {})  # type: ignore
        return super(UserInjected, session_class).__call__(root_folder=root_folder)  # type: ignore
