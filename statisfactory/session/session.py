#! /usr/bin/python3

# session.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements the single entry point to a Statisfactory application
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

from importlib import import_module
from pathlib import Path
from abc import ABCMeta
from statisfactory.errors import Errors
from statisfactory.operator import Scoped
from statisfactory.loader import (
    get_path_to_target,
    get_pyproject,
)

from statisfactory.session import BaseSession

#############################################################################
#                                  Script                                   #
#############################################################################


class _SessionMaker(type):
    """
    A metaclass to dynamically select the appropriate bases for the Session object.
    The custom base to inject inplace of the statisfactory.session.BaseSession can be set through the pyproject.toml file
    """

    def __call__(cls, root_folder=None):
        """
        Return a new Session class type to be used to instanciate Sessions.
        The new class inherits from the user defined one (if provide.)
        """

        # Retrieve the location of the config file
        root = Path(root_folder or get_path_to_target("pyproject.toml"))

        # If no custom factory is defined, then return the base session.
        config = get_pyproject(root / "pyproject.toml")
        if not config.session_factory:
            factory = BaseSession
        else:
            module, obj = config.session_factory.module, config.session_factory.factory  # type: ignore
            factory = getattr(import_module(module), obj)

        # Create a new class inheriting from the factory
        session_class = type(cls.__name__, (cls, factory), {})  # type: ignore
        return super(_SessionMaker, session_class).__call__(root_folder=root_folder)  # type: ignore


class _AbstractSessionMaker(_SessionMaker, ABCMeta):
    """
    BaseSession is an Abtract class. It's metaclass is ABC.
    Session schould have a _SessioNaker metaclass.
    The both metaclass conflict, since pyhton does not now wich one of the metaclasses it shcould be using.
    To solve the conflict, I manually create a Metaclass that inherits from both the ABCMeta metaclass and the _SessionMaker
    """

    ...


class Session(metaclass=_AbstractSessionMaker):
    """
    Instanciate the Session object.
    The Session class used is generaly the `statisfactory.session.base_session`.

    The user can specify a custom session by specifiying the following configuration in the `pyproject.toml` file :

    ```toml
    [tool.statisfactory.session_factory]
    module = 'session.custom_session'
    factory = 'MySession'
    ```

    Where `module` is the name of a module inside the project and factory is the name of a class from that module.

    The Session is the entry point of a Statisfactory application
    The session loads the Statisfactory configuration file and executes the registered hooks.
    """

    @staticmethod
    def get_active_session():
        """
        Get the Session, currently on the top of the stack.
        Must be called from a `with` statement
        """

        S = Scoped().get_session()
        if not S:
            raise Errors.E060() from None  # type: ignore

        return S
