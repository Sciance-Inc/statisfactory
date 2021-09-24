#! /usr/bin/python3

# mixinHookable.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements a mixin to register _pre_hooks, post_hooks and _on_error_hooks
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

from collections import defaultdict
from contextlib import contextmanager
# system
from typing import Callable

# project
from ..logger import get_module_logger

#############################################################################
#                                  Script                                   #
#############################################################################


_LOGGER = get_module_logger("mixinHookable")


class MixinHookable:
    """
    Give to a Class the ability to registrer post, pre and on_error hooks.
    The mixin exposes:
    * A '_with_hooks' context manager executing both, pre (before) and post (after) hooks.
    * A '_with_error' context manager executing the on_error hooks in case of errors
    The hook's signature is described in the hook's dockstring.
    """

    _pre_run_hooks = defaultdict(list)
    _post_run_hooks = defaultdict(list)
    _on_error_hooks = defaultdict(list)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def hook_on_error(cls, last: bool = True) -> Callable:
        """
        Register a `callable_` to be executed if an error is raised during the 'target' execution
        The `callable` must have the signature (target, error) -> None
        """

        def _(callable_: Callable):

            _LOGGER.debug(
                f"Registering {cls.__name__}'s on-error hook : '{callable_.__name__}'"
            )
            if last:
                MixinHookable._on_error_hooks[cls].append(callable_)
            else:
                MixinHookable._on_error_hooks[cls].insert(0, callable_)

            return callable_

        return _

    @classmethod
    def hook_pre_run(cls, last: bool = True) -> Callable:
        """
        Register a `callable_` to be executed before the pipeline execution.
        The `callable` must have the signature (target) -> None
        """

        def _(callable_: Callable):

            _LOGGER.debug(
                f"Registering {cls.__name__}'s pre run's hook : '{callable_.__name__}'"
            )
            if last:
                MixinHookable._pre_run_hooks[cls].append(callable_)
            else:
                MixinHookable._pre_run_hooks[cls].insert(0, callable_)

            return callable_

        return _

    @classmethod
    def hook_post_run(cls, last: bool = True) -> Callable:
        """
        Register a `callable_` to be executed before the pipeline execution.
        The `callable` must have the signature (target) -> None
        """

        def _(callable_: Callable):
            _LOGGER.debug(
                f"Registering {cls.__name__}'s post run's hook : '{callable_.__name__}'"
            )
            if last:
                MixinHookable._post_run_hooks[cls].append(callable_)
            else:
                MixinHookable._post_run_hooks[cls].insert(0, callable_)

            return callable_

        return _

    @contextmanager
    def _with_hooks(self):
        """
        Context manager that executes pre and post hooks.
        """

        for h in MixinHookable._pre_run_hooks[type(self)]:
            self.debug(f"running pre-hook : {h.__name__}")
            h(target=self)

        yield

        for h in MixinHookable._post_run_hooks[type(self)]:
            self.debug(f"running post-hook : {h.__name__}")
            h(target=self)

        return

    @contextmanager
    def _with_error(self):
        """
        Context manager that executes on_error hooks if an error is raised during the context
        """

        try:
            yield
        except BaseException as error:
            for h in MixinHookable._on_error_hooks[type(self)]:
                h(target=self, error=error)


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("mixinHookable.py can't be run in standalone")
