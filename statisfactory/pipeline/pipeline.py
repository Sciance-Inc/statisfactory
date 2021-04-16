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
from typing import Union, Dict
from copy import copy

# Third party
from networkx.algorithms.dag import transitive_reduction

# project
from .solver import DependenciesSolver
from .viz import Graphviz
from .utils import merge_dictionaries, MergeableInterface
from ..errors import errors
from ..logger import MixinLogable


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

        super().__init__()
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

    def __call__(self, **context) -> Dict:
        """
        Run the pipeline with a concrete context.

        Return :
            the volatile state get after applying all crafts

        TODO: inject a runner to multiproc
        """
        # Prepare a dictionary to keep in memory the non-persisten ouput of the successives Crafts
        running_volatile_mapping = {}

        def strict_merge(L, R, kind, craft):
            try:
                return merge_dictionaries(L, R)
            except KeyError as error:
                raise errors.E055(__name__, name=craft.name, kind=kind) from error

        # Sequentially applies the crafts
        cursor = 1
        total = len(self._crafts)
        for batch in DependenciesSolver(self._crafts):

            for craft in batch:

                self.info(f"pipeline : '{self._name}' running craft '{craft.name}'.")

                # Copy the craft (and it's catalog) to make it thread safe
                craft = copy(craft)

                full_context = strict_merge(
                    context, running_volatile_mapping, "context", craft
                )
                try:
                    # Apply the craft and only keep the volatile part
                    volatiles_mapping, _ = craft.call_and_split(**full_context)
                except TypeError as err:
                    raise errors.E052(__name__, func=craft.name) from err
                except BaseException as err:
                    raise errors.E053(__name__, func=craft.name) from err

                # Update the non persisted output
                running_volatile_mapping = strict_merge(
                    volatiles_mapping, running_volatile_mapping, "volatile", craft
                )

                self.info(
                    f"pipeline : '{self._name}' : Completeted {cursor} out of {total} tasks."
                )
                cursor += 1

        return running_volatile_mapping

        self.info(f"pipeline : '{self._name}' succeded.")


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("pipeline.py can't be run in standalone")
