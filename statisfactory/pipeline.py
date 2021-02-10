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
from inspect import Signature, Parameter

# project
from .errors import errors, warnings
from .logger import MixinLogable
from .craft import Craft
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
        self._crafts: Union[Craft] = []
        self._error_on_overwrting = error_on_overwriting

    @property
    def name(self):
        return self._name

    def visit_pipeline(self, pipeline: "Pipeline"):
        """
        Return the first pipeline augmented with the crafts from the current one and an updated context

        Args:
            pipeline (Pipeline): the pipeline to update and return
        """
        self.debug(f"merging pipeline '{self._name}' into '{pipeline._name}'")

        pipeline._crafts.extend(self._crafts)

    def __add__(self, visitor: MergeableInterface) -> "Pipeline":
        """
        Add a Craft to the pipeline
        """

        visitor.visit_pipeline(self)

        return self

    def _build_context(self, craft: Craft, context: Dict, volatile: Dict) -> Dict:
        """
        Return a subset of context with only the keys contained in signature OR all context if the Craft accept a variadic named parameters.
        """
        # Short-circuit : check if **kwargs is in the signature, if we don't have to filter.
        if any(param.kind == "VAR_KEYWORD" for param in craft.signature):
            return context

        # Merge the context with the volatile
        context = self._merge_dictionnaries(
            context, volatile, craft.name, kind="context/volatile"
        )

        # Extract the subset of required params
        out = {}
        for param in craft.signature:
            anno = str(param.annotation) not in Craft._valids_annotations
            kind = param.kind == Parameter.POSITIONAL_OR_KEYWORD
            default = param.default != Parameter.empty
            if kind and anno:
                # Try to fetch the argument in the context.  Raises an error if no value is found and if no defualt is found.
                try:
                    out[param.name] = context[param.name]
                except KeyError:
                    if not default:
                        raise errors.E054(__name__, param=param.name) from None

        return out

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

    def __call__(self, **kwargs) -> Dict:
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

            # Update the craft's catalog context,
            craft.catalog.update_context(**kwargs)

            # Filter the arguments of the craft to only send the expected ones.
            full_context = self._build_context(
                craft, craft.catalog._context, volatile_outputs
            )

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
