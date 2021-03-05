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
from inspect import Parameter
from collections import defaultdict

# Third party
import networkx as nx

# project
from .errors import errors, warnings
from .logger import MixinLogable
from .mergeable import MergeableInterface
from .artefact_interactor import Artefact
from .models import Volatile, Artefact

#############################################################################
#                                  Script                                   #
#############################################################################

class DependenciesSolver():
    """
    DAG dependency solver
    """

    _valids_annotations = (Artefact, Volatile)

    def __init__(self, crafts):
        """
        Return a new Dependency solver

        Implementation details:
            To reduce complexity from O(n^2) to O(n), a mapper is used to invert dependencies so that the graph does not have to be used to search for dependecies
        """

        G = nx.DiGraph()
        m_producer = {}
        
        # First pass, create all nodes and map them to the artefacts they create
        for craft in crafts:
            outputs = (anno.name for anno in craft.output_annotation if isinstance(anno, self._valids_annotations))
            for output in outputs:
                # Check that no craft is already creating this artefact
                if m_producer.get(output, None):
                    raise errors.E056(__name__, artefact=output, L=craft.name, R= m_producer.get(output))
            
                m_producer[output] = craft.name
            
            G.add_node(craft.name)

        # Second pass, drow an edge between all nodes and the craft they require
        for craft in crafts:
            inputs = (anno.name for anno in craft.input_annotation if issubclass(anno.annotation, self._valids_annotations))
            for input_ in inputs:
                try:
                    after = m_producer[input_]
                    G.add_edge(after, craft.name)
                except KeyError:
                    warnings.W056(__name__, craft=craft.name, artefact=input_)

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

    Implementation details
    =====================
    The pipeline works whith shallow copy, not deep one. I choosed shallow copy so that
    I can always copy a Craft or a pipeline without having the user (future me) to implements
    a custom serialiser for each objects in the craft. The shallow copy allows me to replace
    the context of a catalog.
    """

    _valids_annotations = [Artefact]

    def __init__(self, name: str, error_on_overwriting: bool = True):
        """
        Return a new pipeline. The new pipeline execute a list of crafts with only the parametrisation from the catalogue.

        Args:
            name (str): the nmame of the pipeline.
            error_on_overwriting (bool, optional): schould an error be raised when overwritting volatile values. Defaults to True.
        """

        super().__init__()
        self._name = name
        self._crafts: Union["Craft"] = []
        self._error_on_overwrting = error_on_overwriting

    @property
    def name(self):
        return self._name

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

    def _merge_dictionnaries(
        self, left: Dict, right: Dict, craft_name: str, kind: str
    ) -> Dict:
        """
        Return a new dictionnary from right and left. If the Pipeline in strict mode, overwritting keys will raises an error,
        otherwise a warning will be emitted. Overwriting keys option favors right values.
        Args:
            left (Dict): the first dictionnaries to be updated
            right (Dict): The increment to add to the volatile output
            craft_name (str) : the name of the craft, used for debugging
            kind (str) : the type of the merger, used for debugging
        """

        # Prevents dealing with one dict or two being None
        left = left or {}
        right = right or {}

        colliding_keys = set(left.keys()).intersection(set(right.keys()))
        is_collision = len(colliding_keys) > 0

        if is_collision:
            if self._error_on_overwrting:
                raise errors.E055(
                    __name__, keys=",".join(colliding_keys), name=craft_name, kind=kind
                )
            else:
                warnings.W055(
                    __name__, keys=",".join(colliding_keys), name=craft_name, kind=kind
                )

        return {**left, **right}
    
    def __str__(self):
        """
        Implements the print method to display the pipeline
        """

        
        batchs = (batch for batch in DependenciesSolver(self._crafts))
        batchs_repr = "\t - \n".join(", ".join(craft.name for craft in batch) for batch in batchs)
        return "Pipeline steps :\n" + batchs_repr

    def _craft_out_to_dict(self, craft, out):
        """
        Transform the output of a Craft to a dictionary to be mergeable on the context
        """

        if not isinstance(out, (tuple, list)):
            out = out,


        m = {}
        for anno, data in zip(craft.output_annotation, out):
            if isinstance(anno, Artefact):
                pass
            elif isinstance(anno, Volatile):
                m[anno.name] = data

        return m

    def __call__(self, **context) -> Dict:
        """
        Run the pipeline with a concrete context.

        Return :
            the volatile state get after applying all crafts

        TODO: inject a runner to multiproc
        """
        # Prepare a dictionary to keep in memory the non-persisten ouput of the successives Crafts
        volatile_outputs = {}

        # Sequentially applies the crafts
        for batch in DependenciesSolver(self._crafts):

            for craft in batch:

                self.info(f"pipeline : '{self._name}' running craft '{craft.name}'.")

                # Copy the craft (and it's catalog) to make it thread safe
                craft = copy(craft)

                # Combine the volatile dictionary with the context one and send them to the craft
                full_context = self._merge_dictionnaries(context, volatile_outputs, craft.name, "craftContext")

                try:
                    #  Apply the craft
                    out = craft(**full_context)
                except TypeError as err:
                    raise errors.E052(__name__, func=craft.name) from err
                except BaseException as err:
                    raise errors.E053(__name__, func=craft.name) from err

                # Update the non persisted output
                m = self._craft_out_to_dict(craft, out)
                volatile_outputs = self._merge_dictionnaries(
                    volatile_outputs, m, craft.name, "volatile"
                )

        return volatile_outputs

        self.info(f"pipeline : '{self._name}' succeded.")


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("pipeline.py can't be run in standalone")
