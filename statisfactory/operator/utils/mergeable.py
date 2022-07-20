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
# main.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements the mergeableInterface, shared between craft and pipeline
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from abc import ABCMeta, abstractmethod

# project

# third party


#############################################################################
#                                  Script                                   #
#############################################################################


class MergeableInterface(metaclass=ABCMeta):
    """
    Mergeable interface represents the interface toi be satisfiad for object ot be merged.
    The concrete merge operation schould be implemented with a visitor pattern
    """

    def __init__(self, *args, **kwargs):
        """
        Instanciate a new Mergeable, in a inheritable cooperative way.
        """
        super().__init__(*args, **kwargs)

    @abstractmethod
    def visit_pipeline(self, other):
        """
        The "visit" part of the visitor pattern
        """

        raise NotImplementedError("Must be implemented in the concrete class")

    @abstractmethod
    def visit_craft(self, other):
        """
        The "visit" part of the visitor pattern
        """

        raise NotImplementedError("Must be implemented in the concrete class")


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    # (*cough*cough) Smoke-test
    raise BaseException("catalog.py can't run in standalone")
