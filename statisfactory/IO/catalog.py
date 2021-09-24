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
from __future__ import annotations  # noqa

from typing import TYPE_CHECKING, Any

# third party
import pandas as pd

from ..errors import errors
from ..logger import MixinLogable
from .artefact_interactor import ArtefactInteractor
# project
from .models import Artefact, CatalogData, Connector

# Project type checks : see PEP563
if TYPE_CHECKING:
    from ..session import Session

#############################################################################
#                                  Script                                   #
#############################################################################


class Catalog(MixinLogable):
    """
    Catalog represent a loadable / savable set of dataframes living locally or in far, far aways distributed system.
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        """
        Implements a Singleton for the Catalog
        """

        if cls._instance is None:
            cls._instance = super().__new__(cls)

        return cls._instance

    def __init__(
        self,
        *,
        dump: str,
        session: Session = None,
        context_overwrite_strict: bool = True
    ):
        """
        Build a new Catalog from an iterator of dumps

        Params:
            dump (str): The string representaiton of the yaml to be parsed
            session (Session): An optional statisfactory.session to be cascaded to the interactor
        """

        super().__init__(__name__)

        self._context_overwrite_strict = context_overwrite_strict
        self._session = session

        try:
            self._data = CatalogData.from_string(dump)
        except BaseException as err:
            raise errors.E013(__name__, file="Catalog") from err

    def __str__(self):
        """
        Show all artefacts entries
        """

        keys = sorted(self._data.artefacts.keys())
        msg = "\n\t- ".join(keys)

        return "Catalog entries :\n\t- " + msg

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
                raise errors.E032(__name__, name=name)

            conn = connector

        return conn

    def _get_artefact(self, name: str) -> Artefact:
        """
        Retrieve the FIRST artefact matching the given name currently living in the catalog.
        """

        try:
            artefact = self._data.artefacts[name]
        except KeyError:
            raise errors.E030(__name__, name=name)

        return artefact

    def _get_interactor(self, artefact: Artefact) -> ArtefactInteractor:
        """
        Retrieve the interactor matchin the type of the artefact.
        """

        try:
            interactor = ArtefactInteractor.interactors()[artefact.type]
        except KeyError:
            raise errors.E031(__name__, name=artefact.type)

        return interactor

    def load(self, name: str, **context) -> pd.DataFrame:
        """Load an asset from the catalogue.
        A context can be provided through named variadic args.
        if a context is provided, the update of the context won't raised any error

        Args:
            name (str): the name of the artefact to load.
        """

        artefact = self._get_artefact(name)
        connector = self._get_connector(artefact)
        interactor: ArtefactInteractor = self._get_interactor(artefact)(
            artefact=artefact, connector=connector, session=self._session, **context
        )

        return interactor.load()

    def save(self, name: str, asset: Any, **context):
        """Save the asset using the artefact name.
        A context can be provided through named variadic args.
        if a context is provided, the update of the context won't raised any error

        Args:
            name (str): the name of the arteface
            asset (Any): the underlying artefact to store
        """

        artefact = self._get_artefact(name)
        connector = self._get_connector(artefact)
        interactor: ArtefactInteractor = self._get_interactor(artefact)(
            artefact=artefact, connector=connector, session=self._session, **context
        )

        interactor.save(asset)


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    # (*cough*cough) Smoke-test
    raise BaseException("catalog.py can't run in standalone")
