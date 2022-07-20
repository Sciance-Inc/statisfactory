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

# project
from statisfactory.operator.pipeline.solver import DAGSolver
from statisfactory.operator.utils import merge_dictionaries

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

    def _update_volatiles(self, accumulated: Dict[str, Any], craft: _Craft, craft_output: Iterable[Any]) -> Dict[str, Any]:
        """
        Return a new Volatile mapping, from the union of the current `accumulated` mapping with the volatiles extracted from a craft.
        Raise an error if keys collides.
        """

        if not craft_output:
            return accumulated

        # Extract the volatiles from the Craft's outputed values
        update = {anno.name: value for anno, value in zip(craft.output_annotations, craft_output) if anno.kind == AnnotationKind.VOLATILE}

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

    def __call__(self, **kwargs) -> Dict[str, Any]:
        """
        Iterate through the crafts and accumulate the volatiles and context object, starting from a the given ones.

        Args:
            kwargs (Optional[Dict[str, any]]): An optionnal mapping containings configuration to be shared and namespaced configuration to be dispatched to the craft.The __call__ method uses the FQN of a craft to accordingly dispatch the configurations.

        Returns:
            Dict[str, Any]: the final transient state resultuing from the craft application
        """

        # Extract the FQN of the pipeline's craft
        crafts_full_names = set()
        for craft in self:
            craft_module = craft.__module__ if craft.__module__ != "__main__" else None
            craft_full_name = ".".join(filter(None, (craft_module, craft.name)))
            crafts_full_names.add(craft_full_name)

        # Split the Kwargs between shared and namespaced
        namespaced = {k: v for k, v in kwargs.items() if k in crafts_full_names}
        shared = {k: v for k, v in kwargs.items() if k not in crafts_full_names}

        # Initiate a mapping of volatiles values
        running_volatile: Dict[str, Any] = {}

        # Initiate a cursor to keep track of the progress
        cursor = 1

        # Iterate over the craft and accumulate the States
        for craft in self:
            self.info(f"running craft '{craft.name}'.")
            craft_module = craft.__module__ if craft.__module__ != "__main__" else None
            craft_full_name = ".".join(filter(None, (craft_module, craft.name)))

            craft_namespaced_context = namespaced.get(craft_full_name, {})
            if not isinstance(craft_namespaced_context, (Mapping)):
                raise Errors.E055(got=str(type(craft_namespaced_context)))  # type: ignore

            craft_context = {**shared, **craft_namespaced_context}  # type: ignore

            self.info(f"Executing {craft_full_name} with execution context : \n {craft_context}")

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
