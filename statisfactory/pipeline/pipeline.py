#! /usr/bin/python3

# pipeline.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements a way to chain Crafts.
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from typing import Union, Dict, Mapping
from copy import copy

# Third party
from networkx.algorithms.dag import transitive_reduction

# project
from .solver import DependenciesSolver
from .viz import Graphviz
from .utils import merge_dictionaries, MergeableInterface
from ..errors import errors
from ..logger import MixinLogable
from .selements import SElementKind

#############################################################################
#                                  Script                                   #
#############################################################################


class Pipeline(MergeableInterface, MixinLogable):
    """
    Implements a way to run crafs.
    """

    def __init__(self, name: str, error_on_overwriting: bool = True):
        """
        Return a new pipeline. The new pipeline execute a list of crafts with only the parametrisation from the catalogue.

        Args:
            name (str): the nmame of the pipeline.
            error_on_overwriting (bool, optional): schould an error be raised when overwritting volatile values. Defaults to True.
        """

        super().__init__(loger_name=__name__)
        self._name = name
        self._crafts: Union["Craft"] = []  # noqa
        self._error_on_overwrting = error_on_overwriting

    @property
    def name(self):
        """
        Craft's name getter.
        """

        return self._name

    def plot(self):
        """
        Display the graph.
        """

        # Process all the nodes of the graph<
        G = DependenciesSolver(self._crafts)

        # Remove redundants edges
        G = transitive_reduction(G)

        Graphviz(G)

    def __add__(self, visitor: MergeableInterface) -> "Pipeline":
        """
        Add a Mergeable to the pipeline.
        Implements the accept part of the visitor pattern.
        """

        visitor.visit_pipeline(self)
        return self

    def visit_craft(self, craft) -> "Pipeline":
        """
        Insert the given craft in the first position of the pipeline.
        """

        self._crafts.insert(0, craft)
        return self

    def visit_pipeline(self, pipeline: "Pipeline"):
        """
        Update self by adding the crafts from 'pipeline'.

        Args:
            pipeline (Pipeline): the pipeline to update and return
        """
        self.debug(f"merging pipeline '{self._name}' into '{pipeline._name}'")

        pipeline._crafts.extend(self._crafts)

    def __str__(self):
        """
        Implements the print method to display the pipeline
        """
        batchs_repr = "\n\t- ".join(
            ", ".join(craft.name for craft in batch)
            for batch in DependenciesSolver(self._crafts)
        )
        return "Pipeline steps :\n\t- " + batchs_repr

    def __call__(self, **context: Mapping) -> Dict:
        """
        Run the pipeline with a concrete context.

        Arguments:
            context (Mapping): A Mapping parameters to be used through the subsequent call to the crafts.

        Return :
            The final Volatile state.
        """

        # Prepare a running_context to be dispatched to each craft, initiated as the context and updated with the default values returned from the Crafts
        running_context = context

        # Prepare a dictionary to keep in memory the non-persisted ouputs of the successives Crafts
        running_volatile = {}

        # Create an helpers to merge to running context and the running volatile
        def strict_merge(L, R, kind, name):
            """
            Stricly merge to dictionaries and raise an error if keys collide
            """

            try:
                return merge_dictionaries(L, R)
            except KeyError as error:
                raise errors.E052(__name__, name=name, kind=kind) from error

        # Sequentially applies the crafts
        cursor = 1
        total = len(self._crafts)
        for batch in DependenciesSolver(self._crafts):

            for craft in batch:

                self.info(f"pipeline : '{self._name}' running craft '{craft.name}'.")

                # Copy the craft avoid any side effect
                craft = copy(craft)

                try:
                    # Call the craft with the current context and volatile mapping.
                    # Since I'm in a pipeline, there is no variadic arguments
                    out = craft(volatiles_mapping=running_volatile, **running_context)
                except BaseException as err:
                    raise errors.E050(__name__, func=craft.name) from err

                # Convert the output to a tuple
                if not isinstance(out, tuple):
                    out = (out,)

                # Extract the volatiles from the Craft's outputed values
                if out is not None:
                    volatiles = {
                        anno.name: value
                        for anno, value in zip(craft.output_annotations, out)
                        if anno.kind == SElementKind.VOLATILE
                    }

                    running_volatile = strict_merge(
                        volatiles, running_volatile, "volatile", craft.name
                    )

                # Extract the implicit context from the craft signature : since a pipeline does not use variadic positionals arguments, the implicit context is simply the default values of the craft's arguments
                implicit_context = {
                    anno.name: anno.annotation.default
                    for anno in craft.input_annotations
                    if anno.has_default
                }

                # Union the implicit context with the running context, but give priority to running context
                running_context = {**implicit_context, **running_context}

                self.info(
                    f"pipeline : '{self._name}' : Completeted {cursor} out of {total} tasks."
                )
                cursor += 1

        return running_volatile

        self.info(f"pipeline : '{self._name}' succeded.")


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("pipeline.py can't be run in standalone")
