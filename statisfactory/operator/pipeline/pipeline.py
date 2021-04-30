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

# project
from .solver import DAGSolver, Solver
from .runner import Runner
from .viz import Graphviz
from ..scoped import Scoped
from ..mixinHookable import MixinHookable
from ..utils import MergeableInterface
from ...logger import MixinLogable, get_module_logger

#############################################################################
#                                  Script                                   #
#############################################################################

_LOGGER = get_module_logger(__name__)


class Pipeline(Scoped, MixinHookable, MergeableInterface, MixinLogable):
    """
    Implements a way to combine crafts and pipeline togetger
    """

    def __init__(
        self,
        *,
        name: str = "noName",
        runner_name: str = "SequentialRunner",
        solver: Solver = DAGSolver,
    ):
        """
        Configure a new pipeline to execute a list of crafts.
        The Pipeline binds together a concrete runner to run the Crafts and a dependencies solver.
        Craft or Pipeline can be added to the pipeline with the `+` notation.

        Args:
            name (str): The name of the pipeline.
            runner (Runner): The runner to internaly use to execute the crafts
            solver (Solver): The object to use to solve the dependencies between crafts.
        """

        super().__init__(logger_name=__name__)
        self._name = name
        self._runner = Runner.get_runner_by_name(runner_name)
        self._solver = solver

        # A placeholder to holds the added crafts.
        self._crafts: Union["Craft"] = []  # noqa

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
        return Graphviz(self._solver(self._crafts).G)

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

        batches = self._solver(self._crafts)

        batchs_repr = "\n\t- ".join(
            ", ".join(craft.name for craft in batch) for batch in batches
        )

        return "Pipeline steps :\n\t- " + batchs_repr

    def __call__(self, **context: Mapping) -> Dict:
        """
        Run the pipeline with a concrete context.

        Arguments:
            session (Session): The statisfactory session to use f
            context (Mapping): A Mapping parameters to be used through the subsequent call to the crafts.

        Return :
            The final Volatile state.
        """

        # Prepare a running_context to be dispatched to each craft, initiated as the context and updated with the default values returned from the Crafts
        running_context = context

        # Prepare a dictionary to keep in memory the non-persisted ouputs of the successives Crafts
        running_volatile = {}

        # Inject the crafts and the solver into the runner
        runner = self._runner(crafts=self._crafts, solver=self._solver)

        # Call the runner with the Context and the Volatile
        self.info(f"Starting pipeline '{self._name}' execution")

        with self._with_hooks():
            with self._with_error():
                final_state = runner(
                    volatiles=running_volatile, context=running_context
                )

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
