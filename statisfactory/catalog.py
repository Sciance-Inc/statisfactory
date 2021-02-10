#! /usr/bin/python3

# main.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements the single entry point to the datasources
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from pathlib import Path
from typing import Any
from contextlib import contextmanager
from copy import copy

# project
from .logger import MixinLogable
from .errors import errors, warnings
from .models import CatalogData, Artefact, Connector
from .artefact_interactor import (
    CSVInteractor,
    ODBCInteractor,
    XLSXInteractor,
    ArtefactInteractor,
    DatapaneInteractor,
    PicklerInteractor,
)

# third party
from filelock import FileLock
import pandas as pd

#############################################################################
#                                  Script                                   #
#############################################################################


class Catalog(MixinLogable):
    """Catalog represent a loadable / savabale set of dataframes living locally or in far, far aways distributed system.
    The catalog mixes a monitor with a facade pattern (yeup, quick dev)

    TODO : Rework the way catalog'context and pipeline's context are combined : create a new object for context and pass it independlyt
    """

    # Map each artefact type to it's corresponding interactor
    interactorMapper = {
        "odbc": ODBCInteractor,
        "csv": CSVInteractor,
        "xslx": XLSXInteractor,
        "datapane": DatapaneInteractor,
        "pickle": PicklerInteractor,
    }

    def __init__(self, path: str, context_overwrite_strict: bool = True, **kwargs):
        """Build a new catalog for the root 'path'.

        The catalog loads:
            * a "catalog.yaml" file
        """

        super().__init__()
        self.debug("preflight : check...")
        self._context_overwrite_strict = context_overwrite_strict

        # Check that the path exists
        path = Path(path)
        if not path.exists():
            raise errors.E010(
                __name__, path=path
            )  # moups, pas d'erreur dans un init ;)

        # If so, lock the folder
        lock_path = FileLock(path.joinpath("statisfactory.lock"))
        with lock_path:

            # Check that a data folder exists
            data_path = path.joinpath("Data")
            if not data_path.exists() or not data_path.is_dir:
                raise errors.E011

            # Check that the catalog.yaml file exits
            catalog_path = path.joinpath("catalog.yaml")
            if not catalog_path.exists():
                raise errors.E012

            # load it
            try:
                self._data = CatalogData.from_file(catalog_path)
            except BaseException as err:
                raise errors.E013 from err

        # Create a context from the path an any surnumerary arguments.
        context = {"data_path": data_path}
        self._context = {**context, **kwargs}
        self.debug(f"initiaing Catalog with context : '{self._context}'")

        self.debug("preflight : ...ok")

    @contextmanager
    def _temp_context(self, **kwargs):
        """
        Temporaly update the context
        """
        if not kwargs:
            yield
            return

        old = copy(self._context)
        self._context_overwrite_strict = False
        self.update_context(**kwargs)
        yield
        self._context = old
        self._context_overwrite_strict = True
        return

    def update_context(self, **kwargs):
        """
        Update the context of the catalog
        """

        common = set(self._context.keys()).intersection(set(kwargs.keys()))
        if len(common) > 0:
            if self._context_overwrite_strict:
                raise errors.E033(__name__, keys=", ".join(common))
            else:
                warnings.W033(__name__, keys=", ".join(common))

        self._context = {**self._context, **kwargs}

    def __contains__(self, name: str) -> bool:
        """
        Check if the given name is an artefact
        """

        return name in self._data.artefacts.keys()

    def _get_connector(self, artefact: Artefact) -> Connector:
        """
        Retrieve the connector of an Artefact

        Args:
            artefact (Artefact): the artefact to extract the connector for
        """

        name = artefact.connector
        conn = None
        if name:

            for key, connector in self._data.connectors.items():
                if key == name:
                    break
            else:
                raise errors.E032.format(name=name)

            conn = connector

        return conn

    def _get_artefact(self, name: str) -> Artefact:
        """
        Retrieve the FIRST artefact matching the given name currently living in the catalog.
        """

        try:
            artefact = self._data.artefacts[name]
        except KeyError:
            raise errors.E030.format(name=name)

        return artefact

    def _get_interactor(self, artefact: Artefact) -> ArtefactInteractor:
        """
        Retrieve the interactor matchin the type of the artefact.
        """

        try:
            interactor = Catalog.interactorMapper[artefact.type]
        except KeyError:
            raise errors.E031(__name__, name=artefact.type)

        return interactor

    def load(self, name: str, **kwargs) -> pd.DataFrame:
        """Load an asset from the catalogue.
        A context can be provided through named variadic args.
        if a context is provided, the update of the context won't raised any error

        Args:
            name (str): the name of the artefact to load.
        """

        with self._temp_context(**kwargs):
            artefact = self._get_artefact(name)
            connector = self._get_connector(artefact)
            interactor: ArtefactInteractor = self._get_interactor(artefact)(
                artefact=artefact, connector=connector, **self._context
            )

        return interactor.load()

    def save(self, name: str, asset: Any, **kwargs):
        """Save the asset using the artefact name.
        A context can be provided through named variadic args.
        if a context is provided, the update of the context won't raised any error

        Args:
            name (str): the name of the arteface
            asset (Any): the underlying artefact to store
        """

        with self._temp_context(**kwargs):
            artefact = self._get_artefact(name)
            connector = self._get_connector(artefact)
            interactor: ArtefactInteractor = self._get_interactor(artefact)(
                artefact=artefact, connector=connector, **self._context
            )

        interactor.save(asset)


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    # (*cough*cough) Smoke-test
    raise BaseException("catalog.py can't run in standalone")
