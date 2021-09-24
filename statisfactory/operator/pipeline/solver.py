#! /usr/bin/python3

# solver.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements classe to solve the dependencies between crafts
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from __future__ import annotations  # noqa

from abc import ABCMeta, abstractmethod, abstractproperty
from typing import TYPE_CHECKING, Iterable

# Third party
import networkx as nx

from ...errors import errors, warnings
# project
from ...logger import MixinLogable

# Project type checks : see PEP563
if TYPE_CHECKING:
    from ..craft import Craft

#############################################################################
#                                  Script                                   #
#############################################################################


class Solver(MixinLogable, metaclass=ABCMeta):
    """
    Interface for the Solver. A solver compute the Computational Graph.
    """

    def __init__(self, crafts: Iterable[Craft], *args, **kwargs):
        """
        Instanciate a new dependencies Solver
        """

        self._crafts = set(crafts)

    @abstractmethod
    def __iter__(self) -> Iterable[Iterable[Craft]]:
        """
        Implements the iteration protocole for the solver.
        Return an iterator over batch of Crafts
        """

        raise NotImplementedError("must be implemented in the concrete class")

    @abstractproperty
    def G(self) -> nx.DiGraph:
        """
        Get the computational graph as a networkx
        """

        raise NotImplementedError("must be implemented in the concrete class")


class DAGSolver(Solver):
    """
    Implements the solver though the use of DAG
    """

    def __init__(self, crafts):
        """
        Return a new DAG Dependency solver.

        Implementation details:
            To reduce complexity from O(n^2) to O(n), a mapper is used to invert dependencies so that the graph does not have to be used to search for dependecies
        """

        super().__init__(crafts)
        self._name_to_craft = {craft.name: craft for craft in crafts}

    def G(self) -> nx.DiGraph:
        """
        Returns the Computational Directed Graph.
        """

        return self._build_diGraph()

    def _build_diGraph(self) -> nx.DiGraph:  # noqa
        """
        Build the DiGraph from an iterable of crafts.
        """

        G = nx.DiGraph()
        m_producer = {}

        # First pass, create all nodes and map them to the artefacts they create
        for craft in self._crafts:

            # Map the node to the Artefacts and Vilatile
            for output in craft.produces:
                # Check that no craft is already creating this artefact
                if output in m_producer:
                    raise errors.E053(
                        __name__,
                        artefact=output,
                        L=craft.name,
                        R=m_producer.get(output),
                    )

                m_producer[output] = craft.name
            G.add_node(craft.name)

        # Second pass, drow an edge between all nodes and the craft they require
        for craft in self._crafts:
            for requirement in craft.requires:
                try:
                    after = m_producer[requirement]
                    G.add_edge(after, craft.name)
                except KeyError:
                    warnings.W051(__name__, craft=craft.name, artefact=requirement)

        return G

    def __iter__(self):
        """
        Implements a grouped topological sort
        """

        G = self._build_diGraph()

        indegree_map = {v: d for v, d in G.in_degree() if d > 0}
        zero_indegree = [v for v, d in G.in_degree() if d == 0]
        while zero_indegree:
            yield (self._name_to_craft[c] for c in zero_indegree)
            new_zero_indegree = []
            for v in zero_indegree:
                for _, child in G.edges(v):
                    indegree_map[child] -= 1
                    if not indegree_map[child]:
                        new_zero_indegree.append(child)
            zero_indegree = new_zero_indegree


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    raise BaseException("solver.py can't be run in standalone")
