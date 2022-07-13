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
from networkx.algorithms import transitive_reduction

# project
from statisfactory.errors import Errors

# TODO : To be fully reworked : "quick" way to get a graphical representation of the pipeline.
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
            raise Errors.E054(dep="graphviz")  # type: ignore

        try:
            import pygraphviz  # noqa
        except ImportError:
            raise Errors.E054(dep="pygraphviz")  # type: ignore

        # Remove redondencies
        reduction = transitive_reduction(G)

        # Tranform the Graph into an AGraph one
        A = nx.nx_agraph.to_agraph(reduction)
        A.layout("dot")

        return graphviz.Source(A.to_string())


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    raise BaseException("viz.py can't be run in standalone")
