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
from statisfactory.IO.artifacts.artifact_interactor import ArtifactInteractor

from statisfactory.IO.models import Artifact, CatalogData, Connector
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
        Show all artifacts entries
        """

        keys = sorted(self._data.artifacts.keys())
        msg = "\n\t- ".join(keys)

        return "Catalog entries :\n\t- " + msg

    def __contains__(self, name: str) -> bool:
        """
        Check if the given name is an artifact
        """

        return name in self._data.artifacts.keys()

    def _get_connector(self, artifact: Artifact) -> Union[Connector, None]:
        """
        Retrieve the connector of an Artifact

        Args:
            artifact (Artifact): the artifact to extract the connector for
        """

        name = artifact.connector
        conn = None
        if name:

            for key, connector in self._data.connectors.items():
                if key == name:
                    break
            else:
                raise Errors.E032(name=name)  # type: ignore

            conn = connector

        return conn

    def _get_artifact(self, name: str) -> Artifact:
        """
        Retrieve the FIRST artifact matching the given name currently living in the catalog.
        """

        try:
            artifact = self._data.artifacts[name]
        except KeyError:
            raise Errors.E030(name=name)  # type: ignore

        return artifact

    def _get_interactor(self, artifact: Artifact) -> Callable:
        """
        Retrieve the interactor matchin the type of the artifact.
        """

        try:
            interactor = ArtifactInteractor.interactors()[artifact.type]
        except KeyError:
            raise Errors.E031(name=artifact.type)  # type: ignore

        return interactor

    def load(self, name: str, **context) -> pd.DataFrame:
        """Load an asset from the catalogue.
        A context can be provided through named variadic args.
        if a context is provided, the update of the context won't raised any error

        Args:
            name (str): the name of the artifact to load.
        """

        artifact = self._get_artifact(name)
        connector = self._get_connector(artifact)
        interactor: ArtifactInteractor = self._get_interactor(artifact)(
            artifact=artifact, connector=connector, session=self._session, **context
        )

        return interactor.load(**context)

    def save(self, name: str, asset: Any, **context):
        """Save the asset using the artifact name.
        A context can be provided through named variadic args.
        if a context is provided, the update of the context won't raised any error

        Args:
            name (str): the name of the arteface
            asset (Any): the underlying artifact to store
        """

        artifact = self._get_artifact(name)
        connector = self._get_connector(artifact)
        interactor: ArtifactInteractor = self._get_interactor(artifact)(
            artifact=artifact, connector=connector, session=self._session, **context
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

        for k, v in self._data.artifacts.items():
            if k in other._data.artifacts:
                raise Errors.E033(key=k, type="artifact")  # type: ignore
            other._data.artifacts[k] = v

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
