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

    def __init__(self, name: str, catalog: Catalog):
        """
        Return a new pipeline. The new pipeline execute a list of crafts with only the parametrisation from the catalogue.
        """

        super().__init__()
        self._name = name
        self._functors: Union[Craft] = []
        self._catalog = copy(catalog)

    def __add__(self, functor: Callable) -> "Pipeline":
        """
        Add a Craft to the pipeline

        TODO : Maybe use a Visitor to double dispatch between Pipeline and Crafts
        """

        # if not isinstance(craft, Craft):
        #    raise errors.E050(__name__, str(type(craft)))

        try:
            if functor.craft.catalog:
                raise errors.E051(__name__, func=functor.craft.name)
        except errors.E044:
            pass

        # Isolate the craft
        functor = copy(functor)
        functor.craft.catalog = self._catalog

        self._functors.append(functor)

        self.debug(f"adding craft '{functor.craft.name} to the pipeline {self._name}'")

        return self

    def __call__(self, **kwargs):
        """
        Run the pipeline with a concrete context.

        TODO : improve error shunt / switch
        TODO : test interface with direct injection of kwargs into the functor
        """

        # Update the catalog, so that every craft now use the current context.
        self._catalog.update_context(**kwargs)

        for functor in self._functors:
            self.info(
                f"pipeline : '{self._name}' running craft '{functor.craft.name}'."
            )
            try:
                functor()
            except TypeError as err:
                raise errors.E052(__name__, func=functor.craft.name) from err
            except BaseException as err:
                raise errors.E053 from err

        self.info(f"pipeline : '{self._name}' success (or silently failed).")


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("pipeline.py can't be run in standalone")
