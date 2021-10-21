#! /usr/bin/python3

# session.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements the single entry point to a Statisfactory application
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

import os
import sys
from pathlib import Path
from string import Template
from typing import Any, Callable, Mapping, Union
from warnings import warn

import boto3
from dynaconf import Dynaconf, Validator

from ..errors import Errors, Warnings
from ..IO import Catalog
from ..logger import MixinLogable, get_module_logger
from ..operator import Scoped
# project
from .loader import ConfigsLoader, PipelinesLoader

#############################################################################
#                                  Script                                   #
#############################################################################


class _CatalogTemplateParser(Template):
    """
    Template for the Catalog.
    Override the default template to :
        * disallow interpolation of non braced values.
        * Allow the . syntax to namespace the templated values
    """

    delimiter = "$"
    pattern = r"""
    \$(?:
      (?P<escaped>\$) |
      {(?P<named>[_a-z\.][\._a-z0-9]*)} |
      {(?P<braced>[_a-z\.][\._a-z0-9]*)} |
      (?P<invalid>)
    )
    """


class Session(MixinLogable):
    """
    Represents the single entry point to a Statisfactory application.
    Load the Statisfactory configuration file and executes the registered hook.
    The class exposes :
        * Getter to the 'settings', parsed during the class instanciation
        * Getters to pipelines definitions and configurations, if any.
        * A decorator : hook_post_init. To help the user registering is own extention.
    The session expose a '_' attributes, for the user to register his own datas.

    Implementation details:
    * The class by itself does not do much, but delegates must of the work the the hooks.
    * The hooks can be used to develop plugins : such as an integration to mlflow, the instanciation of a Spark session.
    * The '_' attribute schould be used to store user defined custom extension
    """

    _hooks = []

    @staticmethod
    def get_active_session():
        """
        Get the Session, currently on the top of the stack.
        Must be called from a `with` statement
        """

        S = Scoped().get_session()
        if not S:
            raise Errors.E060() from None  # type: ignore

        return S

    def _get_path_to_root(self) -> Path:
        """
        Retrieve the path to the statisfactory.yaml file by executing a "fish pass ;)" from the location of the caller
        """

        # Retrieve the "statisfactory.yaml" file
        root = Path("/")
        trg = Path().resolve()
        while True:
            if (trg / "statisfactory.yaml").exists():
                return trg
            trg = trg.parent
            if trg == root:
                raise Errors.E010()  # type: ignore

    def __init__(self, *, root_folder: str = None):
        """
        Instanciate a Session by searching for the statisfactory.yaml file in the parent folders
        """

        super().__init__(logger_name=__name__)

        # Retrieve the location of the config file
        self._root = Path(root_folder or self._get_path_to_root())

        self.info(f"Initiating Statisfactory to : '{self._root}'")

        # Parse the config_path, to extract the Catalog(s) and the Parameters locations
        self._settings = Dynaconf(
            validators=[
                Validator("configuration", "catalog", must_exist=True),
                Validator("notebook_target", default="jupyter"),
            ],
            settings_files=[self._root / "statisfactory.yaml"],
            load_dotenv=False,
        )

        # Instanciate the 'user space'
        self._ = {}

        # Instanciate placeholders to be filled by mandatory hooks
        self._catalog = None
        self._pipelines_definitions: Union[None, Mapping[str, Any]] = {}
        self._pipelines_configurations: Union[None, Mapping[str, Any]] = {}
        self._aws_session: Union[boto3.Session, None] = None

        # Execute any registered hooks
        for h in Session._hooks:
            h(self)

        self.info("All done ! You are ready to go ! \U00002728 \U0001F370 \U00002728")

    def __enter__(self):
        """
        Push the Session on top of the thread-safe stack
        """

        Scoped.set_session(self)

    def __exit__(self, exception_type, exception_value, exception_traceback):
        """
        Remove the topmost Session of the thread safe stack
        """

        Scoped.set_session(None)

    @property
    def root(self) -> Path:
        """
        Getter for the Root path of the project
        """

        return self._root

    @property
    def settings(self) -> Dynaconf:
        """
        Getter for the session's base setting.
        """

        return self._settings

    @property
    def catalog(self):
        """
        Getter for the session's catalog.
        """

        return self._catalog

    @property
    def pipelines_definitions(self):
        """
        Getter for the session's pipelines definitions.
        """

        return self._pipelines_definitions

    @property
    def pipelines_configurations(self):
        """
        Getter for the session's pipelines definition.
        """

        return self._pipelines_configurations

    @property
    def aws_session(self) -> boto3.Session:
        """
        Getter for the AWS client
        Returns:
            boto3.Session: The session configured via the initiaition hook.
        """

        if not self._aws_session:
            raise Errors.E062()  # type: ignore

        return self._aws_session

    @classmethod
    def hook_post_init(cls, last=True) -> Callable:
        """
        Register a `callable_` to be executed after the session instanciation.
        """

        def _(callable_: Callable):

            LOGGER = get_module_logger(__name__)
            LOGGER.debug(f"Registering session's hook : '{callable_.__name__}'")
            cls._hooks.append(callable_)

            return callable_

        return _


class _DefaultHooks:
    """
    Name spaces for the mandatory Session postinits hooks.

    Hooks will be executed int the order they are declared.

    Implementation details:
    * These hooks use the logger from the session, instead of cust
    """

    @staticmethod
    @Session.hook_post_init()
    def set_path_and_pythonpath(sess: Session) -> None:
        """
        Configure the Path to expose the Lib targets.
        """

        if "sources" not in sess.settings:
            return

        src_path = (sess.root / str(sess.settings.sources)).resolve()

        # Insert Lib into the Path
        if src_path not in sys.path:
            sys.path.insert(0, str(src_path))
            sess.info(f"adding '{sess.settings.sources}' to PATH")

        # Create / update the python path
        try:
            os.environ["PYTHONPATH"]
            warn(Warnings.W010)  # type: ignore
        except KeyError:
            os.environ["PYTHONPATH"] = str(src_path)
            sess.info(f"setting PYTHONPATH to '{sess.settings.sources}'")

    @staticmethod
    @Session.hook_post_init()
    def set_settings(sess: Session) -> None:
        """
        Parse the settings file from the conf/ folder and add the settings to the base statisfactory settings
        """

        # Warn the user if a configuration target is missing
        targets = {
            "globals.yaml": Warnings.W011,
            "locals.yaml": Warnings.W012,
        }

        for target, w in targets.items():
            if not (sess.root / str(sess.settings.configuration) / target).exists():
                warn(w)

        # Fetch all the config file, in the reversed preceding order (to allow for variables shadowing)
        base = sess.root / str(sess.settings.configuration)
        settings = Dynaconf(
            settings_files=[base / target for target in targets.keys()],
            load_dotenv=False,
        )

        sess._settings.update(settings)  # type: ignore

    @staticmethod
    @Session.hook_post_init()
    def set_catalog(sess: Session) -> None:
        """
        Attach the catalog to the session
        """
        # Read the raw catalog
        path = sess.root / str(sess.settings.catalog)
        try:
            with open(path) as f:
                catalog_representation = _CatalogTemplateParser(f.read())
        except FileNotFoundError as error:
            raise Errors.E011(path=path) from error  # type: ignore

        # Render the catalog with the settings
        if sess.settings:
            try:
                catalog_representation = catalog_representation.substitute(
                    sess.settings  # type: ignore
                )
            except BaseException as error:
                raise Errors.E014() from error  # type: ignore

        # Parse the catalog representation
        sess._catalog = Catalog(dump=catalog_representation, session=sess)  # type: ignore

    @staticmethod
    @Session.hook_post_init()
    def set_pipelines_configurations(sess: Session) -> None:
        """
        Parse and attach pipelines configurations to the Session
        """

        if "pipelines_configurations" not in sess.settings:
            sess.warn("No Pipelines configuration to set up.")
            return

        path = (sess.root / str(sess.settings.pipelines_configurations)).resolve()

        configurations = ConfigsLoader.load(path=str(path))

        sess._pipelines_configurations = configurations

    @staticmethod
    @Session.hook_post_init()
    def set_pipelines_definitions(sess: Session) -> None:
        """
        Parse and attach pipelines to the Session
        """
        if "pipelines_definitions" not in sess.settings:
            sess.warn("No Pipelines definitions to set up.")
            return

        path = (sess.root / str(sess.settings.pipelines_definitions)).resolve()

        pipelines = PipelinesLoader.load(path=str(path))

        sess._pipelines_definitions = pipelines

    @staticmethod
    @Session.hook_post_init()
    def set_AWS_client(sess: Session) -> None:
        """
        Configure a Mamazon session.

        TODO : add support for AWS profiles

        Args:
            sess (Session): The statisfactory session
        """

        try:
            sess._aws_session = boto3.Session(
                aws_access_key_id=sess.settings["AWS_ACCESS_KEY"],
                aws_secret_access_key=sess.settings["AWS_SECRET_ACCESS_KEY"],
                region_name=sess.settings.get("AWS_REGION", "us-east-1"),
            )
        except KeyError:
            warn(Warnings.W060)


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    # (*cough*cough) Smoke-test
    raise BaseException("catalog.py can't run in standalone")
