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
from statisfactory.models.models import Artifact, CatalogData, Connector
from statisfactory.loader import get_catalog_data
from statisfactory.logger import MixinLogable

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

    def __init__(self, *, path: Path, session: Session = None):
        """
        Build a new Catalog from an iterator of dumps

        Params:
            dump (str): The string representaiton of the yaml to be parsed
            session (Session): An optional statisfactory.session to be cascaded to the interactor
        """

        super().__init__(__name__)

        self._session = session

        try:
            self._data: CatalogData = get_catalog_data(path, session)
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
        interactor: ArtifactInteractor = self._get_interactor(artifact)(
            artifact=artifact, session=self._session, **context
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
        interactor: ArtifactInteractor = self._get_interactor(artifact)(
            artifact=artifact, session=self._session, **context
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
