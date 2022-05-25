#! /usr/bin/python3
#
#    Statisfactory - A satisfying statistical facotry
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
