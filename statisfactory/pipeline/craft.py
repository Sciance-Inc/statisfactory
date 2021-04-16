#! /usr/bin/python3

# craft.py
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
from dataclasses import dataclass
from enum import Enum, auto
from functools import update_wrapper
from typing import Callable, List, Tuple
from inspect import signature, Signature, Parameter
from collections.abc import Mapping
from copy import copy

# project
from ..errors import errors
from ..logger import MixinLogable
from ..IO import Catalog, Artefact, Volatile
from .pipeline import Pipeline
from .utils import merge_dictionaries, MergeableInterface

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

        return self.annotation.default != Parameter.empty


class Craft(MergeableInterface, MixinLogable):
    """
    Craft wraps a task and take care of data retrieval from / storage to the catalogue.
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

    def _input_to_SElements(self, annos: List[Parameter]) -> List[SElement]:
        """
        Convert a Python's callable annotation, as returned by Signature to a list of SElement
        """

        e = []
        for anno in annos:
            if anno.annotation == Artefact:
                e.append(SElement(anno, SElementKind.ARTEFACT))
            elif anno.annotation == Volatile:
                e.append(SElement(anno, SElementKind.VOLATILE))
            elif anno.kind == Parameter.VAR_POSITIONAL:
                e.append(SElement(anno, SElementKind.VAR_POSITIONAL))
            elif anno.kind == Parameter.VAR_KEYWORD:
                e.append(SElement(anno, SElementKind.VAR_KEYWORD))
            elif anno.kind == Parameter.POSITIONAL_OR_KEYWORD:
                e.append(SElement(anno, SElementKind.POS_OR_KEY))
            elif anno.kind == Parameter.POSITIONAL_ONLY:
                e.append(SElement(anno, SElementKind.POS))
            elif anno.kind == Parameter.KEYWORD_ONLY:
                e.append(SElement(anno, SElementKind.KEY))
            else:
                raise BaseException("Moups !")

        return e

    def _output_to_SElements(self, annos) -> List[SElement]:
        """
        Convert a Python's callable return value to a list of SElement.
        """

        # Make sure that there is items to be parsed
        if annos == Signature.empty:
            return []

        # Make sure that the item to be parsed as iterable
        if isinstance(annos, (Artefact, Volatile)):
            annos = [annos]

        # Make sure that all items returned by the Craft are either Artefact or Volatile
        if any(not isinstance(anno, (Artefact, Volatile)) for anno in annos):
            raise errors.E047(__name__, name=self.name)

        # Convert all items to a SElement
        e = []
        for anno in annos:
            if isinstance(anno, Artefact):
                e.append(SElement(anno, SElementKind.ARTEFACT))
            elif isinstance(anno, Volatile):
                e.append(SElement(anno, SElementKind.VOLATILE))

        return e

    def __init__(self, catalog: Catalog, callable: Callable):
        """
        Wrap a callable in a craft binded to the given catalog.
        """

        super().__init__()

        self._catalog = catalog
        self._callable = callable
        self._name = callable.__name__

        # Parse the signature of the craft
        S = signature(self._callable)
        self._in_anno = self._input_to_SElements(S.parameters.values())
        self._out_anno = self._output_to_SElements(S.return_annotation)

        update_wrapper(self, callable)

    @property
    def name(self) -> str:
        """
        Get the name of the craft
        """

        return self._name

    @property
    def requires(self) -> Tuple[str]:
        """
        Return the name of the volatiles and artefact required by the craft
        """

        return tuple(
            anno.name
            for anno in self._in_anno
            if anno.kind in (SElementKind.ARTEFACT, SElementKind.VOLATILE)
        )

    @property
    def produces(self) -> Tuple[str]:
        """
        Return the names of the volatiles and artefacts produced by the Craft.
        """

        return tuple(anno.name for anno in self._out_anno)

    def _parse_args(self, args, kwargs):
        """
        Map variadic arguments to the named arguments of the Craft's inner callable
        """

        # Transform the variadic arguments to a generator, so that nexting items from it will consume the tokens : the remainer will be sent to VAR_POSITIONAL
        args = (i for i in args)

        # Iter over the Craft's callable signature and map them values from args and kwargs
        args_ = []
        kwargs_ = {}
        for e in self._in_anno:
            # Load the artefact from the catalog catalog
            if e.kind == SElementKind.ARTEFACT:
                kwargs_[e.name] = self._catalog.load(e.name, **kwargs)

            # Load the value from the context
            if e.kind in (SElementKind.VOLATILE, SElementKind.KEY):
                kwargs_[e.name] = kwargs[e.name]

            # Consume the NEXT positional argument
            if e.kind == SElementKind.POS:
                kwargs_[e.name] = next(args)

            # Use the full context
            if e.kind == SElementKind.VAR_KEYWORD:
                try:
                    kwargs = merge_dictionaries(kwargs_, kwargs)
                except KeyError as error:
                    raise errors.E055(
                        __name__, name=self.name, kind="variadic keywords"
                    ) from error

            # Exhaust the generator
            if e.kind == SElementKind.VAR_POSITIONAL:
                args_ = list(args)  # Consume the remaining items

            # Retrieve from Kwargs, otherwise consume the next token. If no token left, check for default values. If there is not, raise an error.
            if e.kind == SElementKind.POS_OR_KEY:

                try:

                    if e.name in kwargs:
                        kwargs_[e.name] = kwargs[e.name]
                    else:
                        kwargs_[e.name] = next(args)

                except StopIteration:
                    if not e.has_default:
                        raise errors.E046(__name__, name=self._name, param=e.name)

        return args_, kwargs_

    def __call__(self, *args, **kwargs):
        """
        Call the underlying callable
        """

        args_, kwargs_ = self._parse_args(args, kwargs)

        try:
            out = self._callable(*args_, **kwargs_)
        except BaseException as err:
            raise errors.E040(__name__, func=self._name) from err

        self._save_artefacts(out, **kwargs)

        return out

    def call_and_split(self, *args, **kwargs) -> Tuple[Mapping, Mapping]:
        """
        Call the underlying callable and split the returned values between Volatile and Artefacts
        """

        # get the tuple returned by the fonction
        out = self.__call__(*args, **kwargs)
        if not isinstance(out, tuple):
            out = (out,)

        volatiles_mapping = {
            anno.name: value
            for anno, value in zip(self._out_anno, out)
            if anno.kind == SElementKind.VOLATILE
        }
        artefacts_mapping = {
            anno.name: value
            for anno, value in zip(self._out_anno, out)
            if anno.kind == SElementKind.ARTEFACT
        }

        return volatiles_mapping, artefacts_mapping

    def __copy__(self) -> "Craft":
        """
        Implements the shallow copy protocol for the Craft.
        Return a craft with a reference to a copied catalog, so that the context can be independtly updated.
        """

        craft = Craft(copy(self._catalog), self._callable)
        return craft

    def _save_artefacts(self, output, **context) -> Mapping:
        """
        Extract and save the artefact of an output

        Args:
            out (Dict): the dictionnary to extract artefacts from.
        """

        # Convert the output to a tuple
        if not isinstance(output, tuple):
            output = (output,)

        # Make sure the lengths matchs
        expected = len(self._out_anno)
        got = len(output)
        if expected != got:
            raise errors.E0401(
                __name__, name=self._name, sign=len(self._out_anno), got=len(output)
            )

        for item, anno in zip(output, self._out_anno):
            if anno.kind == SElementKind.ARTEFACT:
                self._catalog.save(anno.name, item, **context)
                self.debug(
                    f"craft '{self._name}' : capturing artefacts : '{anno.name}'"
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
