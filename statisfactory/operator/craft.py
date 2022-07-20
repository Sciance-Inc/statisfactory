#! /usr/bin/python3
#
#    Statisfactory - A satisfying statistical factory
#    Copyright (C) 2021-2022  Hugo Juhel
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
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
# from __future__ import annotations  # noqa
from functools import update_wrapper
from inspect import Parameter, Signature, signature
from typing import Any, Callable, Dict, List, Mapping, Tuple, Union  # , TYPE_CHECKING
from warnings import warn

from numpy import empty

# project
from statisfactory.errors import Errors, Warnings
from statisfactory.models.models import Artifact, Volatile
from statisfactory.logger import MixinLogable
from statisfactory.operator.annotations import Annotation, AnnotationKind
from statisfactory.operator.mixinHookable import MixinHookable
from statisfactory.operator.pipeline import Pipeline
from statisfactory.operator.scoped import Scoped
from statisfactory.operator.utils import MergeableInterface

# Project type checks : see PEP563
# if TYPE_CHECKING:
#    from ..session import Session

#############################################################################
#                                  Script                                   #
#############################################################################


class _Craft(Scoped, MixinHookable, MergeableInterface, MixinLogable):
    """
    Craft wraps a task and take care of data retrieval from / storage to the catalogue.
    """

    _artifacts_annotation = [Artifact]

    def __init__(self, callable: Callable):
        """
        Wrap a callable in a craft binded to the given catalog.
        """

        super().__init__(logger_name=__name__)

        self._callable = callable
        self._name = callable.__name__

        # Parse the signature of the craft
        S = signature(self._callable)
        self._in_anno = self._input_to_annotations(S.parameters.values())
        self._out_anno = self._output_to_annotation(S.return_annotation)

        update_wrapper(self, callable)

    def _input_to_annotations(self, inputs) -> Tuple[Annotation, ...]:
        """
        Convert a Python\'s callable annotation, as returned by Signature to a list of SElement
        """

        e: List[Annotation] = []
        for anno in inputs:
            if anno.annotation == Artifact:
                e.append(Annotation(anno, AnnotationKind.ARTEFACT))
            elif anno.annotation == Volatile:
                e.append(Annotation(anno, AnnotationKind.VOLATILE))
            elif anno.kind in (Parameter.POSITIONAL_OR_KEYWORD, Parameter.KEYWORD_ONLY):
                e.append(Annotation(anno, AnnotationKind.KEY))
            elif anno.kind == Parameter.VAR_KEYWORD:
                e.append(Annotation(anno, AnnotationKind.VAR_KEY))
            else:
                raise Errors.E045(name=self.name, anno=anno.kind)  # type: ignore

        return tuple(e)

    def _output_to_annotation(self, inputs) -> Tuple[Annotation, ...]:
        """
        Convert a Python\'s callable return value to a list of SElement.
        """

        # Make sure that there is items to be parsed
        if inputs == Signature.empty:
            return tuple([])

        # Make sure that the item to be parsed as iterable
        if isinstance(inputs, (Artifact, Volatile)):
            inputs = [inputs]

        # Make sure that all items returned by the Craft are either Artifact or Volatile
        if any(not isinstance(anno, (Artifact, Volatile)) for anno in inputs):
            raise Errors.E042(name=self.name)  # type: ignore

        # Convert all items to a Annotation
        e: List[Annotation] = []
        for anno in inputs:
            if isinstance(anno, Artifact):
                e.append(Annotation(anno, AnnotationKind.ARTEFACT))  # type: ignore
            elif isinstance(anno, Volatile):
                e.append(Annotation(anno, AnnotationKind.VOLATILE))  # type: ignore

        return tuple(e)

    @property
    def name(self) -> str:
        """
        Get the name of the craft
        """

        return self._name

    @property
    def requires(self) -> Tuple[str, ...]:
        """
        Return the name of the volatiles and artifact required by the craft
        """

        return tuple(anno.name for anno in self._in_anno if anno.kind in (AnnotationKind.ARTEFACT, AnnotationKind.VOLATILE))

    @property
    def produces(self) -> Tuple[str, ...]:
        """
        Return the names of the volatiles and artifacts produced by the Craft.
        """

        return tuple(anno.name for anno in self._out_anno)

    @property
    def output_annotations(self) -> Tuple[Annotation, ...]:
        """
        Returns the full annotations of the items returned by the craft
        """

        return self._out_anno

    @property
    def input_annotations(self) -> Tuple[Annotation, ...]:
        """
        Returns the full annotations of the items ingested by the craft.
        """

        return self._in_anno

    def _parse_args(self, context: Mapping, volatiles_mapping: Mapping) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Parse the Craft\'s arguments against it\'s signature
        """

        # The loading context is the context required to load the artifacts
        # For the artifact, the key_context is the merge of the default's value callable and the craft context (with priority given to craft's context)
        # Extract default values if any
        default_context = {
            anno.name: anno.annotation.default for anno in self._in_anno if anno.has_default and anno.kind == AnnotationKind.KEY
        }

        artifact_context: Dict[str, Any] = {**default_context, **context}

        mapped_parameters: Dict[str, Any] = {}
        for anno in self._in_anno:

            # If the argument is an Artifact, it schould be loaded using the artifact_context
            if anno.kind is AnnotationKind.ARTEFACT:
                try:
                    mapped_parameters[anno.name] = self.get_session().catalog.load(anno.name, **artifact_context)
                except BaseException as error:
                    if not anno.has_default:
                        raise error

                    mapped_parameters[anno.name] = anno.default
                    warn(Warnings.W40.format(name=self._name, artifact=anno.name))  # type: ignore

            # A volatile must be in the the volatile mapping
            if anno.kind is AnnotationKind.VOLATILE:
                mapped_parameters[anno.name] = value = volatiles_mapping.get(anno.name, anno.default)
                if value is Parameter.empty:
                    raise Errors.E041(name=self._name, param=anno.name)  # type: ignore

            # If the argument is KEYWORD only, it must either be in the context or have a default value
            if anno.kind is AnnotationKind.KEY:
                try:
                    mapped_parameters[anno.name] = artifact_context[anno.name]
                except KeyError:
                    Errors.E041(name=self._name, param=anno.name)  # type: ignore

            # If the argument is VAR_KEYWORD, then it schould be every key the context that is not in mapped_parameters
            # VAR_KEYWORD has to be the last item of the anno, so all KEY arguments have already been parsed
            if anno.kind is AnnotationKind.VAR_KEY:
                var_key = {k: v for k, v in context.items() if k not in mapped_parameters}
                mapped_parameters.update(var_key)

        return mapped_parameters, artifact_context

    def __call__(
        self,
        volatiles_mapping: Union[Mapping[str, Any], None] = None,
        **kwargs,
    ):
        """
        Implements the call protocole for the Craft wrapper.
        Load and save the Artifact for the undelryaing callable.
        Defers the call to the underlying artifact.

        Args:
            volatiles_mapping: A mappping of volatiles objects, computed before the craft execution
            kwargs: variadic keywrods arguments to be defered to the underlaying callable.
        """
        # Extract, from args and kwargs, the item required by the craft and and to kwargs_ the default value, if unspecified by kwargs.
        volatiles_mapping = volatiles_mapping or {}
        craft_arguments, artifact_saving_context = self._parse_args(kwargs, volatiles_mapping)

        with self._with_hooks():
            with self._with_error():
                out = self._callable(**craft_arguments)

        self._save_artifacts(output=out, **artifact_saving_context)  # type: ignore

        # Return the output of the callable
        return out  # type: ignore

    def __copy__(self) -> "_Craft":
        """
        Implements the shallow copy protocol for the Craft.

        Return a craft with a reference to a copied catalog, so that the context can be independtly updated.
        """

        craft = _Craft(self._callable)
        return craft

    def _save_artifacts(self, *, output, **context) -> Union[Mapping, None]:
        """
        Extract and save the artifact of an output

        Args:
            out (Dict): the dictionnary to extract artifacts from.
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
                raise Errors.E044(name=self._name)  # type: ignore

        if expected != got:
            raise Errors.E043(name=self._name, sign=len(self._out_anno), got=len(output))  # type: ignore

        session = self.get_session()
        for item, anno in zip(output, self._out_anno):
            if anno.kind == AnnotationKind.ARTEFACT:
                session.catalog.save(anno.name, item, **context)
                self.debug(f"craft '{self._name}' : capturing artifacts : '{anno.name}'")

    def __add__(self, visitor: MergeableInterface):
        """
        Combine two crafts into a pipeline.
        Implements the accept part of the visitor pattern.

        Args:
            visitor (MergeableInterface): the craft / pipeline to combine
        """

        return visitor.visit_craft(self)

    def visit_craft(self, craft: "_Craft"):
        """
        Combine two crafts into a pipeline

        Args:
            craft (Craft): the other craft to add
        """

        return Pipeline() + craft + self  # Since it's called by "visiting", self is actually the Right object in L + R

    def visit_pipeline(self, pipeline: Pipeline):
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
    @_Craft.hook_on_error()
    def propagate(target, error):
        """
        A default hook to buble-up an error encountred while running a Node.
        """

        raise Errors.E040(func=target._name) from error  # type: ignore


def Craft():
    """
    Make a new _Craft
    """

    def _(func: Callable):

        return _Craft(func)

    return _


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("craft.py can't be run in standalone")
