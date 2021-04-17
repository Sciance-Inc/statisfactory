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
from typing import Callable, List, Tuple, Mapping
from inspect import signature, Signature, Parameter
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
            raise errors.E042(__name__, name=self.name)

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

        super().__init__(loger_name=__name__)

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

    def _parse_args(self, args, context, volatile: Mapping = None):
        """
        Map variadic arguments to the named arguments of the Craft's inner callable
        """

        # Copy the context to not alter it outside of the scope
        context = copy(context)

        volatile = volatile or {}

        # Transform the variadic arguments to a generator, so that nexting items from it will consume the tokens : the remainer will extracted as a variadic argument
        args = (i for i in args)

        # For the artefact, the loading_context is the union of the default's value callable and the craft context
        loading_context = {
            anno.name: anno.annotation.default
            for anno in self._in_anno
            if anno.has_default
        }
        loading_context = {**loading_context, **context}

        # Iter over the Craft's callable signature and map them to values from args, kwargs and the volatile mapping.
        args_ = []
        kwargs_ = {}
        default_context = {}  # To track new args
        for e in self._in_anno:

            # If the argument is an Artefact, it schould be loaded using the loading_context
            if e.kind is SElementKind.ARTEFACT:
                kwargs_[e.name] = self._catalog.load(e.name, **loading_context)

            # If the argument is KEYWORD only, it must either be in the context or have a default value
            if e.kind is SElementKind.KEY:
                if e.name in context:
                    kwargs_[e.name] = context[e.name]
                    # Consume the token, so that there is not duplication with VAR_KEY
                    del context[e.name]
                else:
                    if not e.has_default:
                        raise errors.E041(__name__, name=self._name, param=e.name)
                    default_context[e.name] = kwargs_[e.name] = e.annotation.default

            # A volatile must be in the the volatile mapping
            if e.kind is SElementKind.VOLATILE:
                kwargs_[e.name] = volatile[e.name]

            # If the argument is only positional, the argument must be fetched by consuming the next token of "args"
            if e.kind == SElementKind.POS:
                kwargs_[e.name] = next(args)

            # If the argument is variadic position, it schould take all the remaining token from "args"
            if e.kind == SElementKind.VAR_POSITIONAL:
                args_ = list(args)  # Consume the remaining items

            # If the argument is variadic positional, it must take the remaining of the context
            if e.kind == SElementKind.VAR_KEYWORD:
                try:
                    kwargs_ = merge_dictionaries(kwargs_, context)
                except KeyError as error:
                    raise errors.E052(
                        __name__, name=self.name, kind="variadic keywords"
                    ) from error

            # Retrieve from Kwargs, otherwise consume the next token. If no token left, check for default values. If there is not, raise an error.
            if e.kind == SElementKind.POS_OR_KEY:

                try:

                    if e.name in context:
                        kwargs_[e.name] = context[e.name]
                        # Consume the token, so that there is not duplication with VAR_KEY
                        del context[e.name]
                    else:
                        kwargs_[e.name] = next(args)

                except StopIteration:
                    if not e.has_default:
                        raise errors.E041(__name__, name=self._name, param=e.name)

                    # Explicitely add the default value to the context, to make it avaialbe to the Catalog context
                    default_context[e.name] = kwargs_[e.name] = e.annotation.default

        return args_, kwargs_, default_context

    def _call(self, *args, volatile=None, **kwargs):
        """
        Call the underlying callable and return the output as well as the context updated from the default values of the underlying callable.
        """

        # Extract, from args and kwargs, the item required by the craft and and to kwargs_ the default value, if unspecified by kwargs.
        craft_args, craft_kwargs, default_context = self._parse_args(
            args, kwargs, volatile
        )

        try:
            out = self._callable(*craft_args, **craft_kwargs)
        except BaseException as err:
            raise errors.E040(__name__, func=self._name) from err

        # The saving_context is the union of the initial context and the default_context of the craft
        saving_context = {**default_context, **kwargs}
        self._save_artefacts(out, **saving_context)

        # Return the new value to be added to the context
        return out, default_context

    def __call__(self, *args, **kwargs):
        """
        Call the underlying callable and return the output
        """

        # A craft can't access volatile outside of a Pipeline
        out, *_ = self._call(*args, volatile=None, **kwargs)

        return out

    def call_from_pipeline(
        self, *, context: Mapping, volatiles: Mapping = None
    ) -> Tuple[Mapping, Mapping, Mapping]:
        """
        Call the underlying callable and split the returned values splitted between Volatile and Artefacts
        """

        # get the tuple returned by the fonction
        out, default_context = self._call((), volatile=volatiles, **context)
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

        return volatiles_mapping, default_context, artefacts_mapping

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

        # The callable always returns at least one element : None
        if not expected and got == 1:
            if output[0] is None:
                return
            else:
                raise errors.E044(__name__, name=self._name)

        if expected != got:
            raise errors.E043(
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
