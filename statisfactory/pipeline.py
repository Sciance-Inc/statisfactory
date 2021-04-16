#! /usr/bin/python3

# main.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements a way to chain Craft
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from typing import Union, Dict
from copy import copy

# Third party
import networkx as nx
from networkx.algorithms.dag import transitive_reduction

# project
from .errors import errors, warnings
from .logger import MixinLogable
from .mergeable import MergeableInterface
from .utils import merge_dictionaries

#############################################################################
#                                  Script                                   #
#############################################################################


class DependenciesSolver:
    """
    DAG dependency solver
    """

    @property
    def G(self):
        """
        Return the dependency DiGraph.
        """

        return self._G

    def __init__(self, crafts):
        """
        Return a new Dependency solver

        Implementation details:
            To reduce complexity from O(n^2) to O(n), a mapper is used to invert dependencies so that the graph does not have to be used to search for dependecies
        """

        # Remove duplicated crafts
        crafts = list(set(crafts))

        G = nx.DiGraph()
        m_producer = {}

        # First pass, create all nodes and map them to the artefacts they create
        for craft in crafts:

            for output in craft.produces:
                # Check that no craft is already creating this artefact
                if output in m_producer:
                    raise errors.E056(
                        __name__,
                        artefact=output,
                        L=craft.name,
                        R=m_producer.get(output),
                    )

                m_producer[output] = craft.name

            G.add_node(craft.name)

        # Second pass, drow an edge between all nodes and the craft they require
        for craft in crafts:
            for requirement in craft.requires:
                try:
                    after = m_producer[requirement]
                    G.add_edge(after, craft.name)
                except KeyError:
                    warnings.W056(__name__, craft=craft.name, artefact=requirement)

        self._G = G
        self._m_crafts = {craft.name: craft for craft in crafts}

    def __iter__(self):
        """
        Implements a grouped topological sort
        """

        G = self._G

        indegree_map = {v: d for v, d in G.in_degree() if d > 0}
        zero_indegree = [v for v, d in G.in_degree() if d == 0]
        while zero_indegree:
            yield [self._m_crafts[c] for c in zero_indegree]
            new_zero_indegree = []
            for v in zero_indegree:
                for _, child in G.edges(v):
                    indegree_map[child] -= 1
                    if not indegree_map[child]:
                        new_zero_indegree.append(child)
            zero_indegree = new_zero_indegree


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
        self._DAG_recycle = False

        # Placeholder
        self._solver = None

    @property
    def name(self):
        return self._name

    @property
    def solver(self):
        """
        Return the DependenciesSolver
        """

        if not self._solver or self._DAG_recycle:
            self._solver = DependenciesSolver(self._crafts)
            self._DAG_recycle = False

        return self._solver

    def plot(self):
        """
        Plot the graph in GraphViz
        """

        try:
            import graphviz
        except ImportError:
            raise errors.E057(__name__, dep="graphviz")

        try:
            import pygraphviz  # noqa
        except ImportError:
            raise errors.E057(__name__, dep="pygraphviz")

        # get the transitive reduction of the Digraph
        G = self.solver.G
        G = transitive_reduction(G)

        # Tranform the Graph into an AGraph one
        A = nx.nx_agraph.to_agraph(G)
        A.layout("dot")

        return graphviz.Source(A.to_string())

    def __add__(self, visitor: MergeableInterface) -> "Pipeline":
        """
        Add a Mergeable to the pipeline.
        Implements the accept part of the visitor pattern.
        """

        visitor.visit_pipeline(self)

        # Flag the DAG for recycling
        self._DAG_recycle = True

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

        batchs = (batch for batch in self.solver)
        batchs_repr = "\n\t- ".join(
            ", ".join(craft.name for craft in batch) for batch in batchs
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
        for batch in self.solver:

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

        return running_volatile_mapping

        self.info(f"pipeline : '{self._name}' succeded.")


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("pipeline.py can't be run in standalone")
