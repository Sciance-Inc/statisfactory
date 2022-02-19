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
from typing import Union

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


def get_active_session():
    """
    Get the Session, currently on the top of the stack.
    Must be called from a `with` statement
    """

    S = Scoped().get_session()
    if not S:
        raise Errors.E060() from None  # type: ignore

    return S


def Session(*, root_folder: Union[str, Path] = None):
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

    Implementation details
    ----------------------
    The function has the first capitalized to ensure consystency with previous version of Stati.

    Args:
        root_folder (Union[str, Path], optional): _description_. Defaults to None.
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

    return factory(root_folder=root_folder)  # type: ignore
