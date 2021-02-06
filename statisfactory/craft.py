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
from collections.abc import Mapping

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
    Craft wraps a task and take care of data retrieval from / data storage to the catalogue
    """

    _valids_annotations = ["<class 'statisfactory.models.Artefact'>"]

    def __init__(self, catalog: Catalog = None):
        """
        Wrapf a callable with the given catalog
        """

        super().__init__()
        self._catalog = catalog

        # Placeholders
        self._func_name = (
            None
        )  # The name of the underlying function, for error messages.

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

        self._func_name = func.__name__
        update_wrapper(self, func)
        artefacts = self._get_artefacts(func)

        def _(*args, **kwargs):
            try:
                out = func(*args, **kwargs, **artefacts)
            except BaseException as err:
                raise errors.E040(__name__, func=self._func_name) from err

            out = self._capture_artefacts(out)

            return out

        return _

    def _capture_artefacts(self, in_: Mapping) -> Mapping:
        """
        Extract and save the artefact of an output dict

        Args:
            out (Dict): the dictionnary to extract artefacts from.
        """

        self.debug(f"craft : capturing artefacts from '{self._func_name}'")

        if not in_:
            return

        # Only support dictionaries
        if not isinstance(in_, Mapping):
            raise errors.E041(__name__, func=self._func_name, got=str(type(in_)))

        # Iterate over the artefacts and persist the one existing in the catalog.
        # return only the non-persisted items
        out = {}
        artefacts = []
        for name, artefact in in_.items():
            if name in self.catalog:
                try:
                    self.catalog.save(name, artefact)
                except BaseException as err:  # add details about the callable making the bad call
                    raise errors.E043(__name__, func=self._func_name) from err
                artefacts.append(name)
            else:
                out[name] = in_[name]

        if artefacts:
            self.info(
                f"craft : artefacts saved from '{self._func_name}' : '{', '.join(artefacts)}'."
            )

        return out

    def _get_artefacts(self, func: Callable) -> Dict[str, Artefact]:
        """
        Load the artefacts matching a given function.

        Returns:
            Dict[str, Artefact]: a mapping of artefacts
        """

        self.debug(f"craft : loading artefacts for '{self._func_name}'")

        artefacts = {}
        for param in signature(func).parameters.values():
            if str(param.annotation) in Craft._valids_annotations:
                try:
                    artefacts[param.name] = self.catalog.load(param.name)
                except BaseException as err:  # add details about the callable making the bad call
                    raise errors.E042(__name__, func=self._func_name) from err

        if artefacts:
            self.info(
                f"craft : artefacts loaded for '{self._func_name}' : '{' ,'.join(artefacts.keys())}'."
            )

        return artefacts


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("craft.py can't be run in standalone")
