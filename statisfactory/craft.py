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

    _valids_annotations = [Artefact]

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
        
        # Make sure that the output is Iterable 
        out = s.return_annotation
        if isinstance(out, (Artefact, Volatile)):
            out_ = out,
        elif isinstance(out, tuple):
            out_ = out
            if any(not isinstance(item, (Artefact, Volatile)) for item in out_):
                raise errors.E048(__name__, name=self.name)
        else: 
            raise errors.E047(__name__, name=self.name, got=str(type(out_)))
        
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


    def _filter_signature(self, context: Dict) -> Dict:
        """
        Return a subset of context with only the keys contained in signature OR all context if the Craft accept a variadic named parameters.
        """
        # Short-circuit : check if **kwargs is in the signature, if we don't have to filter.
        if any(param.kind == "VAR_KEYWORD" for param in self._in_anno):
            return context

        # Extract the subset of required params
        out = {}
        for param in self._in_anno:
            anno = param.annotation not in Pipeline._valids_annotations
            kind = param.kind == Parameter.POSITIONAL_OR_KEYWORD
            default = param.default != Parameter.empty
            if kind and anno:
                # Try to fetch the argument in the context.  Raises an error if no value is found and if no defualt is found.
                try:
                    out[param.name] = context[param.name]
                except KeyError:
                    if not default:
                        raise errors.E046(
                            __name__, name=self._name, param=param.name
                        ) from None

        return out

    def __call__(self, *args, **context):
        """
        Call the underlying callable
        """

        artefacts = self._load_artefacts(**context)
        context_signature = self._filter_signature(context)
        full = {**artefacts, **context_signature}
        try:
            out = self._callable(*args, **full)
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
            out = out_, 
        else:
            out = out_
        
        if len(out) != len(self._out_anno):
            raise errors.E0401(__name__, name=self._name, sign=len(self._out_anno), got=len(out))

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

    def _load_artefacts(self, **context) -> Dict[str, Artefact]:
        """
        Load the artefacts matching a given function.

        Returns:
            Dict[str, Artefact]: a mapping of artefacts
        """

        self.debug(f"craft : loading artefacts for '{self._name}'")

        artefacts = {}
        for param in self._in_anno:
            if param.annotation in Craft._valids_annotations:
                try:
                    artefacts[param.name] = self.catalog.load(param.name, **context)
                except BaseException as err:  # add details about the callable making the bad call
                    raise errors.E042(__name__, craft=self._name) from err

        if artefacts:
            self.info(
                f"craft : artefacts loaded for '{self._name}' : '{' ,'.join(artefacts.keys())}'."
            )

        return artefacts

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
