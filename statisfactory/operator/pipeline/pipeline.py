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
from typing import TYPE_CHECKING, Any, Dict, List, Union

from statisfactory.logger import MixinLogable
from statisfactory.operator.mixinHookable import MixinHookable
from statisfactory.operator.scoped import Scoped
from statisfactory.operator.utils import MergeableInterface
from statisfactory.operator.pipeline.runner import Runner

# project
from statisfactory.operator.pipeline.solver import DAGSolver
from statisfactory.operator.pipeline.viz import Graphviz

# Project type checks : see PEP563
if TYPE_CHECKING:
    from statisfactory.operator.craft import _Craft

#############################################################################
#                                  Script                                   #
#############################################################################


class Pipeline(Scoped, MixinHookable, MergeableInterface, MixinLogable):
    """
    Implements a way to combine crafts and pipeline togetger
    """

    def __init__(
        self,
        *,
        name: str = "noName",
    ):
        """
        Configure a new pipeline to execute a list of crafts.
        The Pipeline binds together a concrete runner to run the Crafts and a dependencies solver.
        Craft or Pipeline can be added to the pipeline with the `+` notation.

        Args:
            name (str): The name of the pipeline.
        """

        super().__init__(logger_name=__name__)
        self._name = name

        # A placeholder to holds the added crafts.
        self._crafts: List[_Craft] = []  # noqa

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

        # Process all the nodes of the graph
        return Graphviz()(DAGSolver(self._crafts).G())

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

        batches = DAGSolver(self._crafts)

        batchs_repr = "\n\t- ".join(
            ", ".join(craft.name for craft in batch) for batch in batches
        )

        return "Pipeline steps :\n\t- " + batchs_repr

    def __call__(
        self, *, namespaced: Union[None, Dict[str, Dict[str, Any]]] = None, **shared
    ) -> Dict[str, Any]:
        """
        Concretely run a Pipeline with two Mapping of parameters.
        A 'shared' mapping, to be dispatched to all the crafts executed, a 'namespaced' holding optionnal mappings per crafts to be dispatched to their craft.

        Args:
            shared (Union[None, Dict[str, Any]]): A dictionnary of parameters to dispatch to all the Crafts.
            namespaced (Union[None, Dict[str, Dict[str, Any]]]): A dictionnary of parameters namesapced dispatch to specific crafts.

        Returns:
            Dict[str, Any]: the final transient state resultuing from the craft application
        """

        # Prepare a dictionary to keep in memory the non-persisted ouputs of the successives Crafts

        # Inject the crafts and the solver into the runner
        runner = Runner(crafts=self._crafts)

        # Call the runner with the Context and the Volatile
        self.info(f"Starting pipeline '{self._name}' execution")

        with self._with_hooks():
            with self._with_error():
                final_state = runner(shared, namespaced)

        self.info(f"pipeline '{self._name}' succeded.")
        return final_state


class _DefaultHooks:
    """
    Namesapce for default hooks
    """

    @staticmethod
    @Pipeline.hook_on_error()
    def propagate(target, error):
        """
        A default hook to buble-up an error encountred while running a Pipeline.
        """
        raise error


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("pipeline.py can't be run in standalone")
