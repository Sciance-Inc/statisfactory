#! /usr/bin/python3

# context.py
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
from typing import Mapping
from string import Template
from pathlib import Path
import sys
import os

# Third party
from dynaconf import Dynaconf

# project
from .models import StatisfactoryConfig
from ..IO import Catalog
from ..logger import MixinLogable
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


class Context(MixinLogable):
    """
    Represents the single entry point to a Statisfactory application :
    * Load the various configuration
    * Interpolate the settings into the Catalogs data definition
    * Load the Catalog object
    * Execute any required pre-fligh check

    Only one Context per project can exists.
    """

    _instance = None

    def __new__(
        cls,
        *args,
        **kwargs,
    ):
        """
        Load the current context
        """

        if cls._instance is None:
            cls._instance = super().__new__(cls)

        return cls._instance

    def __init__(self, *, root_folder: str = None):
        """
        Instanciate a Context by searching for the statisfactory.yaml file in the parent folders

        TODO : Add Support for Multiples catalogs
        """

        super().__init__(logger_name=__name__)

        # Retrieve the location of the config file
        self._root = Path(root_folder) or self._get_path_to_root()
        self.info(f"Initiating Statisfactory to : '{self._root}'")

        # Parse the config_path, to extract the Catalog(s) and the Parameters locations
        self._stati_config = StatisfactoryConfig.from_file(
            self._root / "statisfactory.yaml"
        )

        # Build the catalog
        self._catalog = self._get_catalog()

        # Add the Lib
        self._add_to_path()

        # Parse the pipeline
        self.info("All done ! You are ready to go ! \U00002728 \U0001F370 \U00002728")

    def _get_catalog(self) -> Catalog:
        """
        Get the Catalog object.
        """

        # Read the raw catalog
        path = self._root / self._stati_config.catalog
        try:
            with open(path) as f:
                catalog_representation = _CatalogTemplateParser(f.read())
        except FileNotFoundError as error:
            raise errors.E011(__name__, path=path) from error

        # Render the catalog with the settings
        settings = self._get_settings()
        if settings:
            try:
                catalog_representation = catalog_representation.substitute(settings)
            except BaseException as error:
                raise errors.E014 from error

        # Parse the catalog representation
        return Catalog(catalog_representation)

    def _get_settings(self) -> Mapping:
        """
        Parse the settings file from the conf/ folder
        """

        # Warn the user if a configuration target is missing
        targets = {
            "globals.yaml": warnings.w011,
            "locals.yaml": warnings.w012,
        }

        for target, w in targets.items():
            if not (self._root / self._stati_config.configuration / target).exists():
                w(__name__)

        # Fetch all the config file, in the reversed preceding order (to allow for variables shadowing)
        base = self._root / self._stati_config.configuration
        settings = Dynaconf(
            settings_files=[base / target for target in targets.keys()],
            load_dotenv=False,
        )

        return settings

    def _add_to_path(self):
        """
        Add the 'path' folder to the PATH and PYTHONPATH environments variables
        """

        if not self._stati_config.sources:
            return

        src_path = self._root / self._stati_config.sources

        # Insert Lib into the Path
        if src_path not in sys.path:
            sys.path.insert(0, src_path)
            self.info(f"adding '{self._stati_config.sources}' to PATH")

        # Create / update the python path
        try:
            os.environ["PYTHONPATH"]
            warnings.W010(__name__)
        except KeyError:
            os.environ["PYTHONPATH"] = str(src_path)
            self.info(f"setting PYTHONPATH to '{self._stati_config.sources}'")

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

    @property
    def catalog(self) -> Catalog:
        """
        Catalog's getter
        """

        return self._catalog


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    # (*cough*cough) Smoke-test
    raise BaseException("catalog.py can't run in standalone")
