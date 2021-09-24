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

        super(StatisfactoryCricial, self).__init__(self.msg.format(**kwargs))
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

        super(SatisfactoryError, self).__init__(self.msg.format(**kwargs))
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

        super(StatisfactoryWarning, self).__init__(self.msg.format(**kwargs))
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
    E010 = "start-up : statisfactory must be called from a folder, or the child of a folder, containing a 'statisfactory.yaml' file"
    E011 = "start-up : the Catalog file does not exists : '{path}'."
    E012 = "start-up : failed to parse the {file} file : {path}"
    E013 = "start-up : failed to unmarshall the {file}"
    E014 = (
        "start-up : failed to interpolate the catalog with the settings from 'conf/'."
    )
    E015 = "start-up : Parsing pipeline '{pip_name}' : failed to import module '{module}'. The path might not be reachable."
    E016 = "start-up : pipelines definition '{name}' embed a reference to an undeclared definition '{ref}'"
    E017 = "start-up : Parsing pipeline '{pip_name}' : failed to import '{craft_name}' Craft from '{module}'. The module is reachable but importing the Craft failed."

    # FS interactors
    E020 = "data interactor : there is already an interactor named '{name}'"
    E021 = "data interactor : failed to read '{method}' '{name}'"
    E022 = "data interactor : failed to save the asset '{name}' with '{method}' "
    E023 = (
        "data interactor : '{interactor}' interactor only accept {accept} : got '{got}'"
    )
    E024 = "data interactor : '{path}' does not exists"
    E025 = "data interactor : failed to connect to {name} connector"
    E026 = "data interactor : failed to execute query {query} agains {name} connector"
    E027 = "data interactor : only not-null string can be interpolated"
    E028 = "data interactor : string '{trg}' is incomplettely formatted, missing parameters from context"

    # Catalog
    E030 = "catalog : the '{name}' artefact  does not exists"
    E031 = "catalog : the '{name}' interactor does not exists"
    E032 = "catalog : the '{name}' connector does not exists"

    # Craft
    E040 = "craft : failed to execute craft '{func}'"
    E041 = "craft : the craft '{name}' is missing mandatory param '{param}'. Use the context to provide the value or add a default to the function's signature."
    E042 = "craft : the craft '{name}'s signature must be a Volatile, Artefact or a Tuple of Volatiles and Artefacts. Got '{got}'"
    E043 = "craft : the craft '{name}' signature expect {sign} items. Got {got}"
    E044 = (
        "craft : the craft '{name}' signature expect nothing but got a not None value."
    )

    # Pipeline
    E050 = "Pipeline : failed to run c raft '{func}'"
    E052 = "Pipeline : '{kind}' keys collides : for craft '{name}'"
    E053 = "Pipeline : Artefact '{artefact}' is produced by Crafts '{L}' and '{R}'"
    E054 = "Pipeline : viz : missing '{dep}' package. Use 'pip install {dep}' to install the required dependencie. "
    E055 = "Pipeline : NameSpacedSequentialRunner expects each craft's context to be mapping. Got '{got}'"

    # Session
    E060 = "Session : A Craft or a Pipeline must be executed in a 'with session:' statment. Use a context manager to execute the Craft / Pipeline."
    E061 = "Session : Session can't be injected in the craft's underlying callables as the Context already contain a param named Session"

    # Ad hoc
    E999 = "out-of-scope : the method is not supported in the current roadmap"


@_raisable_attributes_factory(StatisfactoryWarning)
class Warnings(UserWarning):
    # Instanciation
    W010 = "start-up : PYTHONPATH is already set and won't be overwritted by Statisfactoy : the sources from 'Lib' MIGHT not be reachable"
    w011 = "start-up : no globals.yaml config found. Defaulting to locals.yaml."
    w012 = "start-up : no locals.yaml config found. Catalogs might not be inteprolated."

    # Interactor
    W020 = "data interactor : '{inter_type}' is not a registered interactor."

    # Craft
    W40 = (
        "Craft : the Craft '{name}' failed to load '{artefact}' and has been defaulted."
    )

    # Pipeline
    W050 = "pipeline : keys collision : '{keys}'"
    W051 = (
        "pipeline : Craft '{craft}' requires an out-of-pipeline Artefact '{artefact}'"
    )


errors = Errors()
warnings = Warnings()

if __name__ == "__main__":
    sys.exit()
