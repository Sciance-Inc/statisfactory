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
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Union

import pandas as pd

from statisfactory.errors import Errors
from statisfactory.IO.artefacts.artefact_interactor import ArtefactInteractor

from statisfactory.IO.models import Artefact, CatalogData, Connector
from statisfactory.logger import MixinLogable

from jinja2 import Template

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

    @staticmethod
    def make(*, path: Path, session: Session) -> Catalog:
        """
        Make a new catalog from a Jinja / Yml source and interpolate static values from the session's setting.

        Args:
            path (Path): The path to the source file to load the catalog from
            session (Session): The session to use to render the template

        Returns:
            Catalog: A catalog object
        """

        # Load and render the Jinja template
        try:
            with open(path) as f:
                template = Template(f.read())
        except FileNotFoundError as error:
            raise Errors.E011(path=path) from error  # type: ignore

        # Dynaconf is case insensitve but not jinaj2 : all configuraiton keys are lowercased
        data = {k.lower(): v for k, v in session.settings.to_dict().items()}
        rendered = template.render(data)

        # Deserialized the rendered template agains the Artifact models
        return Catalog(dump=rendered, session=session)

    def __init__(self, *, dump: str, session: Session = None):
        """
        Build a new Catalog from an iterator of dumps

        Params:
            dump (str): The string representaiton of the yaml to be parsed
            session (Session): An optional statisfactory.session to be cascaded to the interactor
        """

        super().__init__(__name__)

        self._session = session

        try:
            self._data = CatalogData.from_string(dump)
        except BaseException as err:
            raise Errors.E013(file="Catalog") from err  # type: ignore

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

    def _get_connector(self, artefact: Artefact) -> Union[Connector, None]:
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
                raise Errors.E032(name=name)  # type: ignore

            conn = connector

        return conn

    def _get_artefact(self, name: str) -> Artefact:
        """
        Retrieve the FIRST artefact matching the given name currently living in the catalog.
        """

        try:
            artefact = self._data.artefacts[name]
        except KeyError:
            raise Errors.E030(name=name)  # type: ignore

        return artefact

    def _get_interactor(self, artefact: Artefact) -> Callable:
        """
        Retrieve the interactor matchin the type of the artefact.
        """

        try:
            interactor = ArtefactInteractor.interactors()[artefact.type]
        except KeyError:
            raise Errors.E031(name=artefact.type)  # type: ignore

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

        return interactor.load(**context)

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

        interactor.save(asset, **context)  # type: ignore

    def __add__(self, other: Any):
        """
        Implements the visitor pattern for the catalog

        Args:
            other (Any): The right object to add
        """

        return other.visit_catalog(self)

    def visit_catalog(self, other: Catalog) -> Catalog:
        """
        Implements the visitor pattern for the catalog. Combining two catalogs results in a merged catalog.

        Args:
            other (Catalog): The other catalogi to combine with

        Raise:
            Errors.E033: if two artifacts / connectors key collide.
        """

        for k, v in self._data.artefacts.items():
            if k in other._data.artefacts:
                raise Errors.E033(key=k, type="artifact")  # type: ignore
            other._data.artefacts[k] = v

        for k, v in self._data.connectors.items():
            if k in other._data.connectors:
                raise Errors.E033(key=k, type="connector")  # type: ignore
            other._data.connectors[k] = v

        return other


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    # (*cough*cough) Smoke-test
    raise BaseException("catalog.py can't run in standalone")
