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
from .errors import errors
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

    def __init__(self, name: str):
        """
        Return a new pipeline. The new pipeline execute a list of crafts with only the parametrisation from the catalogue.
        """

        super().__init__()
        self._name = name
        self._crafts: Union[Craft] = []

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

    def _filter_context(self, signature: Signature, ctx: Dict) -> Dict:
        """
        Return a subset of ctx with only the keys contained in signature OR all context if the Craft accept a variadic named parameters.
        """
        # Short-circuit : check if **kwargs is in the signature
        if any(param.kind == "VAR_KEYWORD" for param in signature):
            return ctx

        # Extract the subset of required params
        out = {}
        for param in signature:
            anno = str(param.annotation) not in Craft._valids_annotations
            kind = param.kind == Parameter.POSITIONAL_OR_KEYWORD
            default = param.default != Parameter.empty
            if kind and anno:
                try:
                    out[param.name] = ctx[param.name]
                except KeyError:
                    if default:
                        pass
                    else:
                        raise errors.E054(__name__, param=param.name) from None

        return out

    def __call__(self, **kwargs):
        """
        Run the pipeline with a concrete context.
        """

        for craft in self._crafts:
            self.info(f"pipeline : '{self._name}' running craft '{craft.name}'.")

            # Copy the craft (and it's catalog) to make it thread safe
            craft = copy(craft)

            # Update the craft's catalog context,
            craft.catalog.update_context(**kwargs)

            # Filter the arguments of the craft to only send the expected ones.
            kwargs_filtered = self._filter_context(craft.signature, kwargs)

            try:
                craft(**kwargs_filtered)
            except TypeError as err:
                raise errors.E052(__name__, func=craft.name) from err
            except BaseException as err:
                raise errors.E053(__name__, func=craft.name) from err

        self.info(f"pipeline : '{self._name}' succeded.")


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("pipeline.py can't be run in standalone")
