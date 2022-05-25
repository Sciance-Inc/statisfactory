#! /usr/bin/python3
#
#    Statisfactory - A satisfying statistical facotry
#    Copyright (C) 2021-2022  Hugo Juhel
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
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
from typing import TYPE_CHECKING, Any, Callable

import pandas as pd

from statisfactory.errors import Errors
from statisfactory.IO.artifacts.artifact_interactor import ArtifactInteractor
from statisfactory.models.models import Artifact
from statisfactory.loader import get_artifacts_mapping
from statisfactory.logger import MixinLogable

# Project type checks : see PEP563
if TYPE_CHECKING:
    from statisfactory.session import BaseSession

#############################################################################
#                                  Script                                   #
#############################################################################


class Catalog(MixinLogable):
    """
    Catalog represent a loadable / savable set of dataframes living locally or in far, far aways distributed system.
    """

    def __init__(self, *, path: Path, session: BaseSession = None):  # type: ignore
        """
        Build a new Catalog from an iterator of dumps

        Params:
            dump (str): The string representaiton of the yaml to be parsed
            session (Session): An optional statisfactory.session to be cascaded to the interactor
        """

        super().__init__(__name__)

        self._session = session

        try:
            self._artifacts = get_artifacts_mapping(path, session)
        except BaseException as err:
            raise Errors.E013(file="Catalog") from err  # type: ignore

    @property
    def artifacts(self):
        return self._artifacts

    def __str__(self):
        """
        Show all artifacts entries
        """

        keys = sorted(self._artifacts.keys())
        msg = "\n\t- ".join(keys)

        return "Catalog entries :\n\t- " + msg

    def __contains__(self, name: str) -> bool:
        """
        Check if the given name is an artifact
        """

        return name in self._artifacts.keys()

    def _get_artifact(self, name: str) -> Artifact:
        """
        Retrieve the FIRST artifact matching the given name currently living in the catalog.
        """

        try:
            artifact = self._artifacts[name]
        except KeyError:
            raise Errors.E030(name=name)  # type: ignore

        return artifact

    def _get_interactor(self, artifact: Artifact) -> Callable:
        """
        Retrieve the interactor matchin the type of the artifact.
        """

        try:
            interactor = ArtifactInteractor.interactors()[artifact.type]  # type: ignore
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
            Errors.E033: if two artifacts keys collide.
        """

        for k, v in self._artifacts.items():
            if k in other._artifacts:
                raise Errors.E033(key=k, type="artifact")  # type: ignore
            other._artifacts[k] = v

        return other


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    # (*cough*cough) Smoke-test
    raise BaseException("catalog.py can't run in standalone")
