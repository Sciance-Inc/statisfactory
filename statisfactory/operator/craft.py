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

from copy import copy
# system
# from __future__ import annotations  # noqa
from functools import update_wrapper
from inspect import Parameter, Signature, signature
from typing import Callable, List, Mapping, Tuple  # , TYPE_CHECKING

# project
from ..errors import errors, warnings
from ..IO import Artefact, Volatile
from ..logger import MixinLogable
from .mixinHookable import MixinHookable
from .pipeline import Pipeline
from .scoped import Scoped
from .selements import SElement, SElementKind
from .utils import MergeableInterface, merge_dictionaries

# Project type checks : see PEP563
# if TYPE_CHECKING:
#    from ..session import Session

#############################################################################
#                                  Script                                   #
#############################################################################


class Craft(Scoped, MixinHookable, MergeableInterface, MixinLogable):
    """
    Craft wraps a task and take care of data retrieval from / storage to the catalogue.
    """

    _artefacts_annotation = [Artefact]

    @staticmethod
    def make():
        """
        Decorator to make a Craft binded to the catalog from a callable
        """

        def _(func: Callable):

            return Craft(func)

        return _

    def __init__(self, callable: Callable):
        """
        Wrap a callable in a craft binded to the given catalog.
        """

        super().__init__(logger_name=__name__)

        self._callable = callable
        self._name = callable.__name__

        # Parse the signature of the craft
        S = signature(self._callable)
        self._in_anno = self._input_to_SElements(S.parameters.values())
        self._out_anno = self._output_to_SElements(S.return_annotation)

        update_wrapper(self, callable)

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

    @property
    def output_annotations(self) -> Tuple[SElement]:
        """
        Returns the full annotations of the items returned by the craft
        """

        return self._out_anno

    @property
    def input_annotations(self) -> Tuple[SElement]:
        """
        Returns the full annotations of the items ingested by the craft.
        """

        return self._in_anno

    def _parse_args(self, *, args, context, volatiles: Mapping = None):
        """
        Map variadic arguments to the named arguments of the Craft's inner callable
        """

        # Copy the context to avoid alterig it outside of the local scope
        context = copy(context)

        # Ensure mapping for volatiles.
        volatiles = volatiles or {}

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
        # To track new args used in the loading context, but never explicited in the context
        implicit_context = {}

        for e in self._in_anno:

            # If the argument is an Artefact, it schould be loaded using the loading_context
            if e.kind is SElementKind.ARTEFACT:
                try:
                    artefact = self.get_session().catalog.load(
                        e.name, **loading_context
                    )
                except BaseException as error:
                    if e.has_default:
                        artefact = e.default
                        warnings.W40(__name__, name=self._name, artefact=e.name)
                    else:
                        raise error

                kwargs_[e.name] = artefact

            # If the argument is KEYWORD only, it must either be in the context or have a default value
            if e.kind is SElementKind.KEY:
                if e.name in context:
                    kwargs_[e.name] = context[e.name]
                    # Consume the token, so that there is not duplication with VAR_KEY
                    del context[e.name]
                else:
                    if not e.has_default:
                        raise errors.E041(__name__, name=self._name, param=e.name)
                    implicit_context[e.name] = kwargs_[e.name] = e.annotation.default

            # A volatile must be in the the volatile mapping
            if e.kind is SElementKind.VOLATILE:
                kwargs_[e.name] = volatiles[e.name]

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
                    implicit_context[e.name] = kwargs_[e.name] = e.annotation.default

        return args_, kwargs_, implicit_context

    def __call__(
        self,
        *args,
        volatiles_mapping: Mapping = None,
        **kwargs,
    ):
        """
        Implements the call protocole for the Craft wrapper.
        Load and save the Artefact for the undelryaing callable.
        Defers the call to the underlying artefact.

        Arguments:
            *args: Variadic arguments to be defered to the underlaying callable
            volatiles_mapping: A mappping of volatiles objects, computed before the craft execution
            **kwargs: variadic keywrods arguments to be defered to the underlaying callable.
        """

        # Extract, from args and kwargs, the item required by the craft and and to kwargs_ the default value, if unspecified by kwargs.
        craft_args, craft_kwargs, implicit_context = self._parse_args(
            args=args, context=kwargs, volatiles=volatiles_mapping
        )

        with self._with_hooks():
            with self._with_error():
                out = self._callable(*craft_args, **craft_kwargs)

        # The saving_context is the union of the initial context and the implicit_context of the craft, since the default values might contains variables for the string interpolation.
        saving_context = {**implicit_context, **kwargs}
        self._save_artefacts(output=out, **saving_context)

        # Return the output of the callable
        return out

    def __copy__(self) -> "Craft":
        """
        Implements the shallow copy protocol for the Craft.
        Return a craft with a reference to a copied catalog, so that the context can be independtly updated.
        """

        craft = Craft(self._callable)
        return craft

    def _save_artefacts(self, *, output, **context) -> Mapping:
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

        session = self.get_session()
        for item, anno in zip(output, self._out_anno):
            if anno.kind == SElementKind.ARTEFACT:
                session.catalog.save(anno.name, item, **context)
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
            Pipeline() + craft + self
        )  # Since it's called by "visiting", self is actually the Right object in L + R

    def visit_pipeline(self, pipeline: MergeableInterface):
        """
        Add the craft in last position to the visiting pipeline
        """
        self.debug(f"adding Craft '{self._name}' into '{pipeline.name}'")

        # Add the craft
        pipeline._crafts.append(self)


class _DefaultHooks:
    """
    Namesapce for default hooks
    """

    @staticmethod
    @Craft.hook_on_error()
    def propagate(target, error):
        """
        A default hook to buble-up an error encountred while running a Node.
        """

        raise errors.E040(__name__, func=target._name) from error


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("craft.py can't be run in standalone")
