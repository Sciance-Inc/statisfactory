#! /usr/bin/python3

# annotations.py
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

from dataclasses import dataclass
from enum import Enum, auto
from inspect import Parameter

# System
from typing import Any

#############################################################################
#                                  Script                                   #
#############################################################################


class AnnotationKind(Enum):
    """
    Admissibles values for signature elements
    """

    ARTEFACT = auto()  # An artefact to load
    VOLATILE = auto()  # A Volatile object from a previously Executed craft
    KEY = auto()


@dataclass
class Annotation:
    """
    Represents a elements of a Craft's signature.
    Holds the underlaying parameter with it's SElementKind.
    The SElement kind is used to implements the strategy in the Craft's _parse_args method.
    """

    annotation: Parameter
    kind: AnnotationKind

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
    raise BaseException("annotations.py can't be run in standalone")
