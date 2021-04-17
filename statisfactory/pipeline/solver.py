#! /usr/bin/python3

# solver.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements dependencies solver on a DiGraph
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from typing import Set

# Third party
import networkx as nx

# project
from ..errors import errors, warnings

#############################################################################
#                                  Script                                   #
#############################################################################


class DependenciesSolver:
    """
    DiGraph dependencies solver
    """

    @property
    def G(self):
        """
        Return the dependency DiGraph.
        """

        return self._G

    def _build_diGraph(self, crafts: Set["Craft"]) -> nx.DiGraph:  # noqa
        """
        Build the DiGraph from an iterable of crafts.
        """

        G = nx.DiGraph()
        m_producer = {}

        # First pass, create all nodes and map them to the artefacts they create
        for craft in crafts:

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
        for craft in crafts:
            for requirement in craft.requires:
                try:
                    after = m_producer[requirement]
                    G.add_edge(after, craft.name)
                except KeyError:
                    warnings.W051(__name__, craft=craft.name, artefact=requirement)

        return G

    def __init__(self, crafts):
        """
        Return a new Dependency solver

        Implementation details:
            To reduce complexity from O(n^2) to O(n), a mapper is used to invert dependencies so that the graph does not have to be used to search for dependecies
        """

        # Remove duplicated crafts
        crafts = set(crafts)

        self._G = self._build_diGraph(crafts)
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


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    raise BaseException("solver.py can't be run in standalone")
