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

# system
from typing import Callable
from string import Template
from pathlib import Path
import sys
import os

# Third party
from dynaconf import Dynaconf, Validator

# project
from .loader import PipelinesLoader, ConfigsLoader
from ..IO import Catalog
from ..logger import MixinLogable, get_module_logger
from ..errors import errors, warnings


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

    __shared_state__ = {}
    _hooks = []

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
                raise errors.E010(__name__)

    def __init__(self, *, root_folder: str = None):
        """
        Instanciate a Session by searching for the statisfactory.yaml file in the parent folders
        """

        self.__dict__ = self.__shared_state__
        if self.__shared_state__:
            self.debug("Re-using already loaded Statisfactory session.")
            return

        super().__init__(logger_name=__name__)

        # Retrieve the location of the config file
        self._root = Path(root_folder or self._get_path_to_root())

        self.info(f"Initiating Statisfactory to : '{self._root}'")

        # Parse the config_path, to extract the Catalog(s) and the Parameters locations
        self._settings = Dynaconf(
            validators=[Validator("configuration", "catalog", must_exist=True)],
            settings_files=[self._root / "statisfactory.yaml"],
            load_dotenv=False,
        )

        # Instanciate the 'user space'
        self._ = None

        # Instanciate placeholders to be filled by mandatory hooks
        self._catalog = None
        self._pipelines_definitions = None
        self._pipelines_configurations = None

        # Execute any registered hooks
        for h in Session._hooks:
            h(self)

        self.info("All done ! You are ready to go ! \U00002728 \U0001F370 \U00002728")

    @property
    def root(self) -> Path:
        """
        Getter for the Root path of the project
        """

        return self._root

    @property
    def settings(self):
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

    @classmethod
    def hook_post_init(cls, callable_: Callable) -> Callable:
        """
        Register a `callable_` to be executed after the session instanciation.
        """

        LOGGER = get_module_logger(__name__)
        LOGGER.debug(f"Registering session's hook : '{callable_.__name__}'")
        cls._hooks.append(callable_)

        return Callable


class _DefaultHook:
    """
    Name spaces for the mandatory Session postinits hooks.

    Hooks will be executed int the order they are declared.

    Implementation details:
    * These hooks use the logger from the session, instead of cust
    """

    @staticmethod
    @Session.hook_post_init
    def set_path_and_pythonpath(ctx: Session) -> None:
        """
        Configure the Path to expose the Lib targets.
        """

        if not ctx.settings.sources:
            return

        src_path = (ctx.root / ctx.settings.sources).resolve()

        # Insert Lib into the Path
        if src_path not in sys.path:
            sys.path.insert(0, str(src_path))
            ctx.info(f"adding '{ctx.settings.sources}' to PATH")

        # Create / update the python path
        try:
            os.environ["PYTHONPATH"]
            warnings.W010(__name__)
        except KeyError:
            os.environ["PYTHONPATH"] = str(src_path)
            ctx.info(f"setting PYTHONPATH to '{ctx.settings.sources}'")

    @staticmethod
    @Session.hook_post_init
    def set_settings(ctx: Session) -> None:
        """
        Parse the settings file from the conf/ folder and add the settings to the base statisfactory settings
        """

        # Warn the user if a configuration target is missing
        targets = {
            "globals.yaml": warnings.w011,
            "locals.yaml": warnings.w012,
        }

        for target, w in targets.items():
            if not (ctx.root / ctx.settings.configuration / target).exists():
                w(__name__)

        # Fetch all the config file, in the reversed preceding order (to allow for variables shadowing)
        base = ctx.root / ctx.settings.configuration
        settings = Dynaconf(
            settings_files=[base / target for target in targets.keys()],
            load_dotenv=False,
        )

        ctx._settings.update(settings)

    @staticmethod
    @Session.hook_post_init
    def set_catalog(ctx: Session) -> None:
        """
        Attach the catalog to the session
        """
        # Read the raw catalog
        path = ctx.root / ctx.settings.catalog
        try:
            with open(path) as f:
                catalog_representation = _CatalogTemplateParser(f.read())
        except FileNotFoundError as error:
            raise errors.E011(__name__, path=path) from error

        # Render the catalog with the settings
        if ctx.settings:
            try:
                catalog_representation = catalog_representation.substitute(ctx.settings)
            except BaseException as error:
                raise errors.E014 from error

        # Parse the catalog representation
        ctx._catalog = Catalog(catalog_representation)

    @staticmethod
    @Session.hook_post_init
    def set_pipelines_configurations(ctx: Session) -> None:
        """
        Parse and attach pipelines configurations to the Session
        """
        if not ctx.settings.pipelines_configurations:
            ctx.warn("No Pipelines configuration to set up.")
            return

        path = (ctx.root / ctx.settings.pipelines_configurations).resolve()

        configurations = ConfigsLoader.load(path=path)

        ctx._pipelines_configurations = configurations

    @staticmethod
    @Session.hook_post_init
    def set_pipelines_definitions(ctx: Session) -> None:
        """
        Parse and attach pipelines to the Session
        """
        if not ctx.settings.pipelines_definitions:
            return

        path = (ctx.root / ctx.settings.pipelines_definitions).resolve()

        pipelines = PipelinesLoader.load(path=path)

        ctx._pipelines_definitions = pipelines


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    # (*cough*cough) Smoke-test
    raise BaseException("catalog.py can't run in standalone")
