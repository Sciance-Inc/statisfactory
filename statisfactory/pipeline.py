#! /usr/bin/python3

# main.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements a way to chain Craft
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from typing import Union, Callable
from copy import copy

# project
from .errors import errors
from .logger import MixinLogable
from .catalog import Catalog
from .craft import Craft

#############################################################################
#                                  Script                                   #
#############################################################################


class Pipeline(MixinLogable):
    """
    Implements a way to run crafs.

    Implementation details
    =====================
    The pipeline works whith shallow copy, not deep one. I choosed shallow copy so that
    I can always copy a Craft or a pipeline without having the user (future me) to implements
    a custom serialiser for each objects in the craft. The shallow copy allows me to replace
    the context of a catalog.
    """

    def __init__(self, name: str):
        """
        Return a new pipeline. The new pipeline execute a list of crafts with only the parametrisation from the catalogue.
        """

        super().__init__()
        self._name = name
        self._crafts: Union[Craft] = []

    def __add__(self, craft: Craft) -> "Pipeline":
        """
        Add a Craft to the pipeline

        TODO : Maybe use a Visitor to double dispatch between Pipeline and Crafts
        TODO : Implement a __copy__ for the functor
        TODO : remove the notion of catalog from the pipeline. Schould be defined as a Craft attribute
        """

        # Check that the craft has a catalog.
        craft.catalog

        self._crafts.append(craft)

        self.debug(f"adding craft '{craft.name} to the pipeline {self._name}'")

        return self

    def __call__(self, **kwargs):
        """
        Run the pipeline with a concrete context.

        TODO : improve error shunt / switch
        TODO : test interface with direct injection of kwargs into the functor
        """

        for craft in self._crafts:
            self.info(f"pipeline : '{self._name}' running craft '{craft.name}'.")

            # Copy the craft (and it's catalog) to make it thread safe
            craft = copy(craft)

            # Update the craft's catalog context,
            craft.catalog.update_context(**kwargs)

            try:
                craft(**kwargs)
            except TypeError as err:
                raise errors.E052(__name__, func=craft.name) from err
            except BaseException as err:
                raise errors.E053(__name__, func=craft.name) from err

        self.info(f"pipeline : '{self._name}' succeded.")


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("pipeline.py can't be run in standalone")
