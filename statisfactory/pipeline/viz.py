#! /usr/bin/python3

# viz.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements Graph Visualizer (potentially various)
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
# Third party
import networkx as nx

# project
from ..errors import errors

#############################################################################
#                                  Script                                   #
#############################################################################


class Graphviz:
    """
    Display a Networkx graph using GraphViz.
    """

    def __call__(self, G):
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

        # Tranform the Graph into an AGraph one
        A = nx.nx_agraph.to_agraph(G)
        A.layout("dot")

        return graphviz.Source(A.to_string())


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    raise BaseException("viz.py can't be run in standalone")
