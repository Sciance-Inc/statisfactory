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

from copy import copy
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Union

from statisfactory.errors import Errors
from statisfactory.logger import MixinLogable
from statisfactory.operator.annotations import AnnotationKind
from statisfactory.operator.utils import merge_dictionaries

# project
from statisfactory.operator.pipeline.solver import DAGSolver

# Third party


# Project type checks : see PEP563
if TYPE_CHECKING:
    from ..craft import _Craft

#############################################################################
#                                  Script                                   #
#############################################################################


class Runner(MixinLogable):
    """
    Implements a way of running multiples crafts defined together in a pipeline
    """

    def __init__(self, *, crafts: List[_Craft]):
        """
        Instanciate the runners, and prepare the Dag resolution
        """

        self._solver = DAGSolver(crafts)
        self._length = len(crafts)
        super().__init__(logger_name=__name__)

    def _update_volatiles(
        self, accumulated: Dict[str, Any], craft: _Craft, craft_output: Iterable[Any]
    ) -> Dict[str, Any]:
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
            if anno.kind == AnnotationKind.VOLATILE
        }

        # Merge the accumulated with the output
        try:
            updated = merge_dictionaries(accumulated, update)
        except KeyError as error:
            raise Errors.E052(name=craft.name, kind="volatile") from error  # type: ignore

        return updated

    def __iter__(self):
        """
        Unesting generator over the batchs returned by the DAG solver.
        """

        for batch in self._solver:  # type: ignore
            for craft in batch:
                yield copy(craft)

    def __call__(
        self, namespaced: Union[None, Dict[str, Dict[str, Any]]] = None, **shared
    ) -> Dict[str, Any]:
        """
        Iterate through the crafts and accumulate the volatiles and context object, starting from a the given ones.

        Args:
            shared (Union[None, Dict[str, Any]]): A dictionnary of parameters to dispatch to all the Crafts.
            namespaced (Union[None, Dict[str, Dict[str, Any]]]): A dictionnary of parameters namesapced dispatch to specific crafts.

        Returns:
            Dict[str, Any]: the final transient state resultuing from the craft application
        """

        # Set default for dictionnaries
        namespaced = namespaced or {}

        # Initiate a mapping of volatiles values
        running_volatile: Dict[str, Any] = {}

        # Initiate a cursor to keep track of the progress
        cursor = 1

        # Iterate over the craft and accumulate the States
        for craft in self:
            self.info(f"running craft '{craft.name}'.")

            # Extract the parameters for this Craft from it's full name : module + craft's name :
            craft_module = craft.__module__ if craft.__module__ != "__main__" else None
            craft_full_name = ".".join(filter(None, (craft_module, craft.name)))

            # Build the craft execution context from the shared context and the craft's one
            craft_namespaced_context = namespaced.get(craft_full_name, {})
            if not isinstance(craft_namespaced_context, (Mapping)):
                raise Errors.E055(got=str(type(craft_namespaced_context)))  # type: ignore

            craft_context = {**shared, **craft_namespaced_context}  # type: ignore

            self.info(
                f"Executing {craft_full_name} with execution context : \n {craft_context}"
            )

            try:
                output = craft(volatiles_mapping=running_volatile, **craft_context)
            except BaseException as err:
                raise Errors.E050(func=craft.name) from err  # type: ignore

            # Convert the output to a tuple
            if not isinstance(output, tuple):
                output = (output,)

            # Accumulate the running volatiles
            running_volatile = self._update_volatiles(running_volatile, craft, output)

            self.info(f"Completed {cursor} out of {self._length} tasks.")
            cursor += 1

        return running_volatile


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    raise BaseException("solver.py can't be run in standalone")
