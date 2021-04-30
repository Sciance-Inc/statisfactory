#! /usr/bin/python3

# selements.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements classes to retrive signatures values from signature's default and variadic arguments.
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# System
from typing import Any
from dataclasses import dataclass
from enum import Enum, auto
from inspect import Parameter


#############################################################################
#                                  Script                                   #
#############################################################################


class SElementKind(Enum):
    """
    Admissibles values for signature elements
    """

    ARTEFACT = auto()  # An artefact to load
    VOLATILE = auto()  # A Volatile object from a previously Executed craft
    VAR_POSITIONAL = auto()  # A variadic positional argument (*args)
    VAR_KEYWORD = auto()  # A variadic named arguments (**kwargs)
    POS = auto()
    KEY = auto()
    POS_OR_KEY = auto()


@dataclass
class SElement:
    """
    Represents a elements of a Craft's signature.
    Holds the underlaying parameter with it's SElementKind.
    The SElement kind is used to implements the strategy in the Craft's _parse_args method.
    """

    annotation: Parameter
    kind: SElementKind

    @property
    def name(self) -> str:
        """
        Parameter's name getter.
        """
        return self.annotation.name

    @property
    def has_default(self) -> bool:
        """
        Return true if the parameter has a default value.
        """

        return self.annotation.default is not Parameter.empty

    @property
    def default(self) -> Any:
        """
        Annotation's default value getter
        """

        return self.annotation.default


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("craft.py can't be run in standalone")
