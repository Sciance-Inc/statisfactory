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
import warnings

# Project related packages
from .logger import get_module_logger

#############################################################################
#                                Constants                                  #
#############################################################################

DEFAULT_LOGGER = get_module_logger(__name__)
PROJECT_NAME = "Statisfactory"

#############################################################################
#                                 Classes                                   #
#############################################################################


class ExceptionFactory(type):
    """
    Implements a metaclass building errors from instances attributes. Errors are singleton and can be raised and catched.

    >>raise Errors.E010
    >>raise Errors.E010()
    """

    def __init__(cls, name, bases, attrs, *args, **kwargs):
        super().__init__(name, bases, attrs)
        super().__setattr__("_instance", None)
        super().__setattr__("_CACHED_ATTRIBUTES", dict())

    def __call__(cls, *args, **kwargs):
        if super().__getattribute__("_instance") is None:
            super().__setattr__("_instance", super().__call__(*args, **kwargs))
        return super().__getattribute__("_instance")

    def __getattribute__(cls, code):
        """
        Intercept the attribute getter to wrap the Error code in a metaclass. By doing so, the error code became
        a proper class for which the name is the error code
        """

        try:
            meta = super().__getattribute__("_CACHED_ATTRIBUTES")[code]
        except KeyError:

            # Retrieve the error message maching the code and preformat it
            msg = super().__getattribute__(code)
            msg = f"{PROJECT_NAME} : {code} - {msg}"

            proto = super().__getattribute__("_PROTOTYPE")
            meta = type(code, (proto,), {"msg": msg})
            super().__getattribute__("_CACHED_ATTRIBUTES")[code] = meta

        return meta


class ErrorPrototype(Exception):
    """
    Base parent for all custom errors raised by the program.
    The class performs base operation making the error message displaybale
    """

    msg: str = ""

    def __init__(self, **kwargs):

        super().__init__(self.msg.format(**kwargs))


class WarningPrototype(UserWarning):
    """
    Base parent for all custom warnings raised by the program.
    The class performs base operation making the warning displayba;e
    """

    msg: str = ""

    def __init__(self, **kwargs):

        super().__init__(self.msg.format(**kwargs))


class Errors(metaclass=ExceptionFactory):

    _PROTOTYPE = ErrorPrototype

    # Init and connection related errors
    E010 = "start-up : statisfactory must be called from a folder, or the child of a folder, containing a '{target}' file / folder"
    E011 = "start-up : failed to validate the 'statisfactory' section of the 'pyproject.toml'"
    E012 = "start-up : failed to read the pyproject.toml file located here : '{path}'."
    E013 = "start-up : failed to load the catalogs."
    E014 = (
        "start-up : failed to interpolate the catalog with the settings from 'conf/'."
    )
    E015 = "start-up : Parsing pipeline '{pip_name}' : failed to import module '{module}'. The path might not be reachable."
    E016 = "start-up : pipelines definition '{name}' embed a reference to an undeclared definition '{ref}'"
    E017 = "start-up : Parsing pipeline '{pip_name}' : failed to import '{craft_name}' Craft from '{module}'. The module is reachable but importing the Craft failed."
    E0181 = "ConfigParser : '{path}' does not exist."
    E0182 = "ConfigParser : failed to read the '{path}'. Is the file a valid jinja2 template ?"
    E0183 = "ConfigParser : failed to render the '{path}' template with template variables '{vars}'."
    E0184 = "ConfigParser : failed deserialize the yaml representation '{repr}'."

    # FS interactors
    E020 = "data interactor : there is already an interactor named '{name}'"
    E0201 = "data interactor: there is already a backend name registered for the '{prefix}'s prefix."
    E021 = "data interactor : failed to read '{method}' '{name}'"
    E022 = "data interactor : failed to save the asset '{name}' with '{method}' "
    E023 = (
        "data interactor : '{interactor}' interactor only accept {accept} : got '{got}'"
    )
    E024 = "data interactor : '{path}' does not exists"
    E025 = "data interactor : failed to connect to the connector with connection sting '{dsn}'"
    E026 = "data interactor : failed to execute query {query}."
    E027 = "data interactor : only not-null string can be interpolated"
    E028 = "data interactor : string '{trg}' is incomplettely formatted, missing parameters from context"
    E0281 = "data interactor: failed to parse the Path parameter for Artifact {name}."
    E0290 = "data interactor : {backend} failed to serialize or write the payload."
    E0291 = "data interactor : {backend} failed to retrieve, fetch or deserialize the payload. Make sure that the ressource exists in the current branch."
    E0292 = "data interactor : scheme {scheme} does not map to any backend."
    E0293 = "data interactor : LakeFS interactor : the branch's name must match match the regex {regex}."

    # Catalog
    E030 = "catalog : the '{name}' artifact  does not exists"
    E031 = "catalog : the '{name}' interactor does not exists"
    E032 = "catalog : the '{name}' connector does not exists"
    E033 = "catalog : duplicated key. The '{key}' {type} key is defined in at least two catalogs"
    E034 = "catalog : failed to validate the artifact '{name}' Extra key against the following schema : '{schema}'."

    # Craft
    E040 = "craft : failed to execute craft '{func}'"
    E041 = "craft : the craft '{name}' is missing mandatory param '{param}'. Use the context to provide the value or add a default to the function's signature."
    E042 = "craft : the craft '{name}'s signature must be a Volatile, Artifact or a Tuple of Volatiles and Artifacts. Got '{got}'"
    E043 = "craft : the craft '{name}' signature expect {sign} items. Got {got}"
    E044 = (
        "craft : the craft '{name}' signature expect nothing but got a not None value."
    )
    E045 = "Craft : Crafts only support Artifact, Volatile and Keyword only parameters. Craft '{name}' got '{anno}'."

    # Pipeline
    E050 = "Pipeline : failed to run craft '{func}'"
    E052 = "Pipeline : '{kind}' keys collides : for craft '{name}'"
    E053 = "Pipeline : Artifact '{artifact}' is produced by Crafts '{L}' and '{R}'"
    E054 = "Pipeline : viz : missing '{dep}' package. Use 'pip install {dep}' to install the required dependencie. "
    E055 = "Pipeline : NameSpacedSequentialRunner expects each craft's context to be mapping. Got '{got}'"

    # Session
    E060 = "Session : A Craft or a Pipeline must be executed in a 'with session:' statment. Use a context manager to execute the Craft / Pipeline."
    E061 = "Session : Session can't be injected in the craft's underlying callables as the Context already contain a param named Session"
    E062 = "Session : The AWS session has not be configured. You must provide access_key and secret_key through globals / locals configurations files for the session to be instanciated. Hence, statisfactory can't use S3."
    E063 = "Session : The lakeFS client has not be configured. You must provide lakefs_access_key, lakefs_secret_access_key and lakefs_endpoint through globals / locals configurations files for the client to be instanciated. Hence, statisfactory can't use LakeFS."
    E064 = "Session : The git repository has not been configured yet."
    E065 = "Session : there was an error trying to communicate the LakeFS API."

    # Cli
    E070 = "Run : the required parameters set does not exists : '{parameters_name}'"
    E071 = "Run : the required pipeline does not exists : '{pipeline_name}'"

    # Ad hoc
    E999 = "out-of-scope : the method is not supported in the current roadmap"


class Warnings(UserWarning):

    _PROTOTYPE = WarningPrototype

    # Instanciation
    W010 = "start-up : PYTHONPATH is already set and won't be overwritted by Statisfactoy : the sources from 'Lib' MIGHT not be reachable"
    W011 = "start-up : no globals.yaml config found. Defaulting to locals.yaml."
    W012 = "start-up : no locals.yaml config found. Catalogs might not be inteprolated."

    # Interactor
    W020 = "data interactor : '{inter_type}' is not a registered interactor."
    W021 = "S3Backend : no 'aws_s3_endpoint' found in the configuration. Defaulting to AWS."

    # Craft
    W40 = (
        "Craft : the Craft '{name}' failed to load '{artifact}' and has been defaulted."
    )

    # Pipeline
    W050 = "Pipeline : keys collision : '{keys}'"
    W051 = (
        "Pipeline : Craft '{craft}' requires an out-of-pipeline Artifact '{artifact}'"
    )

    # Session
    W060 = "Session : the AWS client was not configured, as either (or both) aws_access_key and aws_secret_access_key were not found in the Globals / Locals configurations files."
    W061 = "Session : the LakeFS client was not configured, as either (or both) lakefs_access_key, lakefs_secret_access_key and lakefs_endpoint were not found in the Globals / Locals configurations files."


def _custom_formatwarning(msg, *args, **kwargs) -> str:
    """
    Monkey patch the warning displayor to avoid printing the code longside the Warnings.
    Monkeypatching the formatter is acutalyy the way to do it as recommanded by Python's documention.
    """
    return f"Warning : {str(msg)} \n"


warnings.formatwarning = _custom_formatwarning


if __name__ == "__main__":
    sys.exit()
