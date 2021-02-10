#! /usr/bin/python3

# errors.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
Centralize errors for the statisfactory package
"""
# pylint: disable-all ## The code is too weird ;(
#############################################################################
#                                 Packages                                  #
#############################################################################

# System packages
import sys

# Project related packages
from .logger import get_module_logger


#############################################################################
#                                Constants                                  #
#############################################################################
DEFAULT_LOGGER = get_module_logger(__name__)

#############################################################################
#                                 Classes                                   #
#############################################################################


# Helpers classes, to make errror class raisble


class Singleton(type):
    """
    Implements a singleton pattern as a metaclass.
    Used on a <error> class, this metaclass provides the ability to raise and catch this class.
    The python's Try/Except mechansims is not based on equality comparison, but on object comparison : to be catched, an object must be the very same than the one raised
    Being a new instance of the same class is not enough. So, I use a singleton to ensure that
    the Errors() instanciation always return the same object (a monostate pattern wouldn't be enough, since it's only share state).
    """

    def __init__(cls, name, bases, attrs, *args, **kwargs):
        super().__init__(name, bases, attrs)
        cls._instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class StatisfactoryCricial(Exception):
    """
    Base parent for all custom critical errors raised by the program.
    The class performs base operation making the critical displayables
    """

    def __init__(self, *args, **kwargs):

        super(Exception, self).__init__(self.msg.format(**kwargs))
        if args:
            logger = get_module_logger(mod_name=args[0])
        else:
            logger = DEFAULT_LOGGER
        logger.critical(str(self))  # Automatically logs the waning


class SatisfactoryError(Exception):
    """
    Base parent for all custom errors raised by the program.
    The class performs base operation making the error message displaybale
    """

    def __init__(self, *args, **kwargs):

        super(Exception, self).__init__(self.msg.format(**kwargs))
        if args:
            logger = get_module_logger(mod_name=args[0])
        else:
            logger = DEFAULT_LOGGER
        logger.error(str(self))  # Automatically logs the error


class StatisfactoryWarning(UserWarning):
    """
    Base parent for all custom warnings raised by the program.
    The class performs base operation making the warning displayba;e
    """

    def __init__(self, *args, **kwargs):

        super(UserWarning, self).__init__(self.msg.format(**kwargs))
        if args:
            logger = get_module_logger(mod_name=args[0])
        else:
            logger = DEFAULT_LOGGER
        logger.warning(str(self))  # Automatically logs the waning


def _raisable_attributes_factory(parent):
    """
    Decorate a class to transform her attributes as error or warning class.
    The python's Try/Except mechansims is not based on equality comparison, but on object comparison : to be catched,
    an object must be the very same than the one raised. Being a new instance of the same class won't work.
    So, I use a caching mechanism on the attributes getter, to ensure that only ONE metaclass per error code would be imstancoated.

    Arguments:
        parent {class} -- The base parent for the attributes : a class deriving from either an exception or a warning
        logging_strategy {class} The strategy to use for the logging : for instance LOGGER.warning, or LOGGER.error
    """

    def wrapper(cls):
        class ExceptionWithCode(cls):
            cls._CACHED_ATTRIBUTES = dict()

            # Ensure that only one metaclass will be created per error : if the same attribute is called, then the first one created will be recalled
            def __getattribute__(self, code):
                """
                Intercept the attrribute getter to wrap the Error code in a metaclass. By doing so, the error code became
                a proper class for which the name is the error code
                """

                try:
                    meta = getattr(cls, "_CACHED_ATTRIBUTES")[
                        code
                    ]  # Yep ! there is a difference betweeen getattr and __getattribute__ ;)
                except KeyError:

                    # Retrieve the error message maching the code and preformat it
                    msg = getattr(cls, code)
                    msg = f"statisfactory : {code} - {msg}"

                    meta = type(code, (parent,), {"msg": msg})
                    getattr(cls, "_CACHED_ATTRIBUTES")[code] = meta

                return meta

        return ExceptionWithCode

    return wrapper


# Updatable class to store errors message and their corresponding warnings
@_raisable_attributes_factory(StatisfactoryCricial)
class Critical(metaclass=Singleton):

    # Failure to start
    pass


@_raisable_attributes_factory(SatisfactoryError)
class Errors(metaclass=Singleton):

    # Init and connection related errors
    E010 = "start-up : '{path}' is not a folder"
    E011 = "start-up : statisfactory required a 'data' folder"
    E012 = "start-up : missing 'catalog.yam' file"
    E013 = "start-up : failed to unmarshall the catalog"

    # FS interactors
    E021 = "data interactor : failed to read '{method}' '{path}'"
    E022 = "data interactor : failed to save the dataframe with '{method}' to '{path}'"
    E023 = "data interactor : MixinLocalFileSystem : want not none 'data_path' and 'path'. got : '{data_path}' and '{path}' "
    E024 = "data interactor : '{path}' does not exists"
    E025 = (
        "data interactor : '{interactor}' interactor only accept {accept} : got '{got}'"
    )
    E026 = "data interactor : path '{path}' is incomplettely formatted, missing parameters from context"
    E027 = "data interactor : failed to connect to {name} connector"
    E028 = "data interactor : failed to execute query {query} agains {name} connector"
    E029 = "data interactor : failed to save the DataPane object '{name}'"

    # Catalog
    E030 = "catalog : the '{name}' artefact  does not exists"
    E031 = "catalog : the '{name}' interactor does not exists"
    E032 = "catalog : the '{name}' connector does not exists"
    E033 = "catalog : context update : key conflict : '{keys}'"

    # Craft
    E040 = "craft : failed to execute callable '{func}'"
    E041 = "craft : the object outputed by {func} schould be a dict. Got : {got}"
    E042 = "craft : failed to load artefact for callable '{func}'"
    E043 = "craft : failed to save artefact for callable '{func}'"
    E044 = "craft : the craft '{func}' has no catalog attached to it"
    E045 = "craft : the craft '{func}' has already a catalog. Here, have some data immutability !"

    # Pipeline
    E050 = "pipeline : other is of type '{other}' and not a Craft"
    E051 = "pipeline : craft '{func}' has already a catalog setted. Remove-it from the craft definition"
    E052 = "pipeline : failed to run craft '{func}'. Make sure that not required parameters are present in the callable definition"
    E053 = "pipeline : failed to run craft '{func}'"
    E054 = "pipeline : missing mandatory param '{param}'. Use the context to provide the value or add a default to the function's signature."
    E055 = "pipeline : '{kind}' keys collision : '{keys}' for craft '{name}'"

    # Ad hoc
    E999 = "out-of-scope : the method is not supported in the current roadmap"


@_raisable_attributes_factory(StatisfactoryWarning)
class Warnings(UserWarning):

    # Pipeline
    W055 = "pipeline : '{kind}' keys collision : '{keys}' for craft '{name}'"


errors = Errors()
warnings = Warnings()

if __name__ == "__main__":
    sys.exit()
