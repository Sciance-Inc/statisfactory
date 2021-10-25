#! /usr/bin/python3

# session.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Thread-safe session setter / getter.
    Used in conjonction with the "with Session.load():" syntax, it allows any inherited class to get the current session at runtime.
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
import threading

# project
from statisfactory.errors import Errors

# Third party


#############################################################################
#                                  Script                                   #
#############################################################################


class Scoped:
    """
    Thread-safe session getter.
    """

    _sessions = threading.local()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_session(self):
        if Scoped._sessions.session is None:
            raise Errors.E060()  # type: ignore

        return Scoped._sessions.session

    @classmethod
    def set_session(cls, session):
        cls._sessions.session = session


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    raise RuntimeError("can't be executed in standalone")
