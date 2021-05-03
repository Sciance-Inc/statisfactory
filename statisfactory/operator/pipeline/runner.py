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
from typing import Mapping, Iterable, Any, TYPE_CHECKING
from abc import ABCMeta, abstractmethod
from copy import copy

# Third party

# project
from .solver import Solver
from ..utils import merge_dictionaries
from ..selements import SElementKind
from ...errors import errors
from ...logger import MixinLogable

# Project type checks : see PEP563
if TYPE_CHECKING:
    from ..craft import Craft

#############################################################################
#                                  Script                                   #
#############################################################################


class Runner(MixinLogable, metaclass=ABCMeta):
    """
    Represents an Interface for the Runners and provide Runners with helpers
    A Runner executes a Computational Graph and track the state.
    """

    _runners = {}
    _logger_name = "runner"

    def __init__(self, *, crafts: Iterable[Craft], solver: Solver):
        """
        Instanciate the runners, and prepare the Dag resolution
        """

        self._solver = solver(crafts)
        self._length = len(crafts)
        super().__init__(logger_name=__name__ + "." + self._logger_name)

    def __init_subclass__(cls, runner_name: str, *args, **kwargs):
        """
        Register the childs runner into the runners dict.
        See PEP-487 for details.
        """

        super().__init_subclass__()
        Runner._runners[runner_name] = cls

    @classmethod
    def get_runner_by_name(cls, name):
        return cls._runners[name]

    @abstractmethod
    def __call__(self, *, volatiles: Mapping = None, context: Mapping = None):  # noqa
        """
        Execute a serie of crafts.
        """

        raise NotImplementedError("must be implemented in the concrete class")

    def _update_volatiles(
        self, accumulated: Mapping, craft: Craft, craft_output: Iterable[Any]
    ) -> Mapping:
        """
        Return a new Volatile mapping, from the union of the current `accumulated` mapping with the volatiles extracted from a craft.
        Raise an error if keys collides.
        """

        if not craft_output:
            return accumulated

        # Extract the volatiles from the Craft's outputed values
        update = {
            anno.name: value
            for anno, value in zip(craft.output_annotations, craft_output)
            if anno.kind == SElementKind.VOLATILE
        }

        # Merge the accumulated with the output
        try:
            updated = merge_dictionaries(accumulated, update)
        except KeyError as error:
            raise errors.E052(__name__, name=craft.name, kind="volatile") from error

        return updated


class SequentialRunner(Runner, runner_name="SequentialRunner"):
    """
    Execute a DAG in a sequential way.
    """

    _logger_name = "sequentialRunner"

    def __iter__(self):
        """
        Unesting generator over the batchs returned by the DAG solver.
        """

        for batch in self._solver:
            for craft in batch:
                yield copy(craft)

    def _update_context(self, accumulated: Mapping, craft: Craft) -> Mapping:
        """
        Return a new Context mapping, from the union of the current `accumulated`
        mapping with the default values of the craft's annotation.
        """

        # Extract the implicit context from the craft signature : since a pipeline does not use variadic positionals arguments, the implicit context is simply the default values of the craft's arguments
        # TODO : implements this filter in the future anno classes hierarchies : DO NOT ADD to the implicit context the artifacts nor volatiles values
        implicit_context = {
            anno.name: anno.default
            for anno in craft.input_annotations
            if anno.has_default and anno.kind.value >= 3
        }

        # Union the implicit context with the running context, but give priority to running context
        updated = {**implicit_context, **accumulated}

        return updated

    def __call__(self, *, volatiles: Mapping = None, context: Mapping = None):
        """
        Iterate through the crafts and accumulate the volatiles and context object, starting from a the given ones.
        """

        # Initiate the two states to their default values if none is provided.
        running_volatile = volatiles or {}
        running_context = context or {}

        # Initiate a cursor to keep track of the progress
        cursor = 1

        # Iterate over the craft and accumulate the States
        for craft in self:
            self.info(f"running craft '{craft.name}'.")

            try:
                # The pipeline's call to a craft does not use any variadic arguments
                output = craft(volatiles_mapping=running_volatile, **running_context)
            except BaseException as err:
                raise errors.E050(__name__, func=craft.name) from err

            # Convert the output to a tuple
            if not isinstance(output, tuple):
                output = (output,)

            # Accumulate the running volatiles and context
            running_volatile = self._update_volatiles(running_volatile, craft, output)
            running_context = self._update_context(running_context, craft)

            self.info(f"Completeted {cursor} out of {self._length} tasks.")
            cursor += 1

        return running_volatile


class NameSpacedSequentialRunner(Runner, runner_name="NameSpacedSequentialRunner"):
    """
    Execute a DAG in a sequential way with NamedSpaced parameters.

    The NameSpaced runner expects the context to be a map of crafts.name to crafts.parameters.
    Implicit context is NOT forwarded to the next craft.
    """

    _logger_name = "NameSpacedSequentialRunner"

    def __iter__(self):
        """
        Unesting generator over the batchs returned by the DAG solver.
        """

        for batch in self._solver:
            for craft in batch:
                yield copy(craft)

    def _get_implicit_context(self, craft: Craft) -> Mapping:
        """
        Extract the implicit context of a Craft.
        """

        # Extract the implicit context from the craft signature : since a pipeline does not use variadic positionals arguments, the implicit context is simply the default values of the craft's arguments
        implicit_context = {
            anno.name: anno.default
            for anno in craft.input_annotations
            if anno.has_default
        }

        return implicit_context

    def __call__(
        self,
        *,
        volatiles: Mapping = None,
        context: Mapping[Mapping, Any] = None,
    ):  # noqa
        """
        Iterate through the crafts and accumulate the volatiles and context object, starting from a the given ones.
        """

        # Initiate the two states to their default values if none is provided.
        running_volatile = volatiles or {}
        context = context or {}

        # Initiate a cursor to keep track of the progress
        cursor = 1

        # Iterate over the craft and accumulate the States
        for craft in self:
            self.info(f"running craft '{craft.name}'.")

            # Extract the parameters for this Craft, and merge (overwrite) the running implicit context
            name_spaced_context = context.get(craft.name, {})
            if not isinstance(name_spaced_context, (Mapping)):
                raise errors.E055(__name__, got=str(type(name_spaced_context)))

            try:
                # The pipeline's call to a craft does not use any variadic arguments
                output = craft(
                    volatiles_mapping=running_volatile, **name_spaced_context
                )
            except BaseException as err:
                raise errors.E050(__name__, func=craft.name) from err

            # Convert the output to a tuple
            if not isinstance(output, tuple):
                output = (output,)

            # Accumulate the running volatiles
            running_volatile = self._update_volatiles(running_volatile, craft, output)

            self.info(f"Completeted {cursor} out of {self._length} tasks.")
            cursor += 1

        return running_volatile


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    raise BaseException("solver.py can't be run in standalone")
