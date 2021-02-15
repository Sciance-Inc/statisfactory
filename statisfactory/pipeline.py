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

# project
from .errors import errors, warnings
from .logger import MixinLogable
from .mergeable import MergeableInterface

#############################################################################
#                                  Script                                   #
#############################################################################


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

    _valids_annotations = ["<class 'statisfactory.models.Artefact'>"]

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

    def __call__(self, **context) -> Dict:
        """
        Run the pipeline with a concrete context.

        Return :
            the volatile state get after applying all craftsé
        """
        # Prepare a dictionary to keep in memory the non-persisten ouput of the successives Crafts
        volatile_outputs = {}

        # Sequentially applies the crafts
        for craft in self._crafts:
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
            volatile_outputs = self._merge_dictionnaries(
                volatile_outputs, out, craft.name, "volatile"
            )

        return volatile_outputs

        self.info(f"pipeline : '{self._name}' succeded.")


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("pipeline.py can't be run in standalone")
