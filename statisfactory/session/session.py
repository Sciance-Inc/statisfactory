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


from statisfactory.errors import Errors
from statisfactory.operator import Scoped
from statisfactory.session import BaseSession
from statisfactory.internals import UserInjected, Prototype

#############################################################################
#                                  Script                                   #
#############################################################################


_session_prototype = Prototype(
    default_factory=BaseSession, factory_name="session_factory"
)


class Session(metaclass=UserInjected, prototype=_session_prototype):
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
