#! /usr/bin/python3

# main.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    implements a wrapper to handler assets loading / saving for an arbitrary function.
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from functools import update_wrapper
from typing import Callable, Dict, Any
from inspect import signature, Signature, Parameter
from collections.abc import Mapping
from copy import copy

# project
from .errors import errors
from .logger import MixinLogable
from .catalog import Catalog
from .models import Artefact, Volatile
from .mergeable import MergeableInterface
from .pipeline import Pipeline

#############################################################################
#                                  Script                                   #
#############################################################################


class Craft(MergeableInterface, MixinLogable):
    """
    Craft wraps a task and take care of data retrieval from / data storage to the catalogue
    """

    _artefacts_annotation = [Artefact]

    @staticmethod
    def make(catalog: Catalog):
        """
        Decorator to make a Craft binded to the catalog from a callable

        Args:
            catalog (Catalog): the catalog to bind the craft to.
            args: the output fields
        """

        def _(func: Callable):

            return Craft(catalog, func)

        return _

    def _extract_annotations(self, c: Callable):
        """
        Extract all the input parameters and a tuple of Volatile / Artefact from the annotated return of the callable.
        """

        s = signature(c)

        # Extract the input parameters to flag the artefacts
        in_ = list(s.parameters.values())

        # Check that a return annotation exists
        out = s.return_annotation
        if out == Signature.empty:
            return in_, []

        # Make sure that the output is Iterable
        if isinstance(out, (Artefact, Volatile)):
            out_ = (out,)
        elif isinstance(out, tuple):
            out_ = out
            if any(not isinstance(item, (Artefact, Volatile)) for item in out_):
                raise errors.E048(__name__, name=self.name)
        else:
            raise errors.E047(__name__, name=self.name, got=str(type(out)))

        return in_, out_

    def __init__(self, catalog: Catalog, callable: Callable):
        """
        Wrap a callable in a craft binded to the given catalog.
        """

        super().__init__()

        self._catalog = catalog
        self._callable = callable
        self._name = callable.__name__
        self._in_anno, self._out_anno = self._extract_annotations(callable)

        update_wrapper(self, callable)

    @property
    def input_annotation(self):
        """
        Return the input annotation of the Craft
        """

        return self._in_anno

    @property
    def output_annotation(self):
        """
        Return the output annotation of the Craft
        """

        return self._out_anno

    def _build_args(self, context: Dict) -> Dict:
        """
        Build the dictionnary mapping from a Craft's signature
        """

        built_args = {}
        artefact_only = False
        loaded_artefacts = []

        # Short-circuit : check if **kwargs is in the signature, if so, we only have to load the artefacts
        if any(param.kind == "VAR_KEYWORD" for param in self._in_anno):
            built_args = context
            artefact_only = True

        for param in self._in_anno:
            if param.annotation in self._artefacts_annotation:
                try:
                    built_args[param.name] = self.catalog.load(param.name, **context)
                    loaded_artefacts.append(param.name)
                except BaseException as err:
                    raise errors.E042(__name__, craft=self._name) from err
            else:
                if artefact_only:
                    continue
                if param.kind == Parameter.POSITIONAL_OR_KEYWORD:
                    try:
                        built_args[param.name] = context[param.name]
                    except KeyError:
                        is_required = param.default == Parameter.empty
                        if is_required:
                            raise errors.E046(
                                __name__, name=self._name, param=param.name
                            ) from None

        if loaded_artefacts:
            self.info(
                f"craft : artefacts loaded for '{self._name}' : {', '.join(loaded_artefacts)}."
            )

        return built_args

    def __call__(self, *args, **context):
        """
        Call the underlying callable
        """

        params = self._build_args(context)
        try:
            out = self._callable(*args, **params)
        except BaseException as err:
            raise errors.E040(__name__, func=self._name) from err

        self._capture_artefacts(out, **context)

        return out

    def __copy__(self) -> "Craft":
        """
        Implements the shallow copy protocol for the Craft.
        Return a craft with a reference to a copied catalog, so that the context can be independtly updated.
        """

        craft = Craft(copy(self._catalog), self._callable)
        return craft

    @property
    def name(self) -> str:
        """
        Get the name of the craft
        """

        return self._name

    @property
    def catalog(self):
        """
        Getter for the catalog
        """

        return self._catalog

    def _capture_artefacts(self, out_, **context) -> Mapping:
        """
        Extract and save the artefact of an output

        Args:
            out (Dict): the dictionnary to extract artefacts from.
        """

        self.debug(f"craft : capturing artefacts from '{self._name}'")

        # Ensure iterable
        if not isinstance(out_, (tuple, list)):
            out = (out_,)
        else:
            out = out_

        if len(out) != len(self._out_anno):
            raise errors.E0401(
                __name__, name=self._name, sign=len(self._out_anno), got=len(out)
            )

        artefacts = []
        for anno, data in zip(self._out_anno, out):
            if isinstance(anno, Artefact):
                try:
                    self._catalog.save(anno.name, data, **context)
                except BaseException as err:  # add details about the callable making the bad call
                    raise errors.E043(__name__, func=self._name) from err
                artefacts.append(anno.name)

        # Iterate over the artefacts and persist the one existing in the catalog.

        if artefacts:
            self.info(
                f"craft : artefacts saved from '{self._name}' : {', '.join(artefacts)}."
            )

    def __add__(self, visitor: MergeableInterface):
        """
        Combine two crafts into a pipeline.
        Implements the accept part of the visitor pattern.

        Args:
            visitor (MergeableInterface): the craft / pipeline to combine
        """

        return visitor.visit_craft(self)

    def visit_craft(self, craft: "Craft"):
        """
        Combine two crafts into a pipeline

        Args:
            craft (Craft): the other craft to add
        """

        return (
            Pipeline("noName") + craft + self
        )  # Since it's called by "visiting", self is actually the Right object in L + R

    def visit_pipeline(self, pipeline: MergeableInterface):
        """
        Add the craft in last position to the visiting pipeline
        """
        self.info(f"adding craft {self._name} into {pipeline.name}")

        # Add the craft
        pipeline._crafts.append(self)


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("craft.py can't be run in standalone")
