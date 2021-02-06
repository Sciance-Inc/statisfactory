#! /usr/bin/python3

# main.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    implemts a wrapper to handler assets loading / saving for a given craf
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from functools import update_wrapper
from typing import Callable, Dict
from inspect import signature

# project
from .errors import errors
from .logger import MixinLogable
from .catalog import Catalog
from .models import Artefact

#############################################################################
#                                  Script                                   #
#############################################################################


class Craft(MixinLogable):
    """
    Craft wraps a task and take care of data retrieval from the catalogue
    """

    _valids_annotations = ["<class 'statisfactory.models.Artefact'>"]

    def __init__(self, catalog: Catalog = None):
        """
        Wrapf a callable with the given catalog
        """

        super().__init__()
        self._catalog = catalog

    @property
    def catalog(self):
        """
        TODO : implements None checks
        """

        return self._catalog

    def __call__(self, func, *args, **kwargs):
        """
        Decorate the called func
        """

        update_wrapper(self, func)
        artefacts = self._get_artefacts(func)

        def _(*args, **kwargs):
            return func(*args, **kwargs, **artefacts)

        return _

    def _get_artefacts(self, func: Callable) -> Dict[str, Artefact]:
        """
        Load the artefacts matching a given function.

        Returns:
            Dict[str, Artefact]: a mapping of artefacts
        """

        artefacts = {}
        for param in signature(func).parameters.values():
            if str(param.annotation) in Craft._valids_annotations:
                artefacts[param.name] = self.catalog.load(param.name)

        if artefacts:
            self.debug(
                f"Craft : extracted '{artefacts.keys()}' from function's signature."
            )

        return artefacts


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("craft.py can't be run in standalone")
