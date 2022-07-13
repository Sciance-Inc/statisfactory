#! /usr/bin/python3
#
#    Statisfactory - A satisfying statistical factory
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
    implements various data interactor the catalog can delegates the saving / loading to.
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from __future__ import annotations

import pickle
import tempfile
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from inspect import Parameter, signature
from io import BytesIO  # noqa
from pathlib import Path
from string import Template
from typing import TYPE_CHECKING, Any, Callable, Dict, Union
from urllib.parse import urlparse

import pyarrow.feather as feather
from pydantic.dataclasses import dataclass
from pydantic import ValidationError

import pandas as pd  # type: ignore
import pyodbc  # type: ignore

from statisfactory.errors import Errors
from statisfactory.IO.artifacts.backend import Backend

# project
from statisfactory.logger import MixinLogable, get_module_logger

# Project type checks : see PEP563
if TYPE_CHECKING:
    from statisfactory.IO.artifacts.backend import Backend
    from statisfactory.session import BaseSession

#############################################################################
#                                  Script                                   #
#############################################################################


class DynamicInterpolation(Template):
    """
    Implements the interpolation of the !{} for the artifact values.

    Override the default template to :
        * replace the $ used by the StaticInterpolation with ! (the @ might be used in connection string)
        * disallow interpolation of non braced values.
    """

    delimiter = "!"
    pattern = r"""
    \!(?:
      (?P<escaped>\!) |
      {(?P<named>[_a-z][_a-z0-9]*)} |
      {(?P<braced>[_a-z][_a-z0-9]*)} |
      (?P<invalid>)
    )
    """  # type: ignore


class MixinInterpolable:
    """
    Implements helpers to interpolate a string
    """

    def __init__(self, *args, **kwargs):
        """
        Instanciate the MixinInterpolable
        """

        super().__init__(*args, **kwargs)

    def _interpolate_string(self, string, **kwargs):
        """
        Interpolate a given string using the provided context
        """

        if not string:
            raise Errors.E027()  # type: ignore

        try:
            string = DynamicInterpolation(string).substitute(**kwargs)
        except KeyError as err:
            raise Errors.E028(trg=string) from err  # type: ignore

        return string


class ArtifactInteractor(MixinLogable, MixinInterpolable, metaclass=ABCMeta):
    """
    Describe the Interactor's interface.
    An interactor wraps the loading and saving operations of a Artifact.
    The user can implements custom interactors. To do so, the user should
    implements the interface desbribes in this class. An artifact and a Session object
    are available when the artifact is called by the Catalog.

    Validation of the Extra parameter is possible through the definition of an inner class named Extra.
    The inner class schould be a pydantic dataclasse or a pydantic base model to allow for automatic validation.
    """

    Extra = None

    # A placeholder for all registered interactors
    _interactors = dict()

    def __init__(self, artifact, *args, session: BaseSession, **kwargs):
        """
        Instanciate a new interactor.

        The interactors implements cooperative inheritance, only the artifact is mandatory.
        """

        super().__init__(logger_name=__name__, *args, **kwargs)
        self.name = artifact.name
        self._save_options = artifact.save_options
        self._load_options = artifact.load_options
        self._session = session

        # Mutate the artifact extra fields with a validated schema against the custom inner class.
        # Since this is a mutation, the conversion to extra must only be done once
        if self.Extra and isinstance(artifact.extra, Dict):
            try:
                artifact.extra = self.Extra(**artifact.extra)
            except (ValidationError, TypeError) as error:
                schema = self.Extra.__pydantic_model__.schema()["properties"]
                raise Errors.E034(name=artifact.name, schema=schema) from error  # type: ignore

    def __init_subclass__(cls, interactor_name, register: bool = True, **kwargs):
        """
        Implement the registration of a child class into the artifact class.
        By doing so, the ArtifactInteractor can be extended to use new interactors without updating the code of the class (Open Close principle)
        See PEP-487 for details.
        """

        super().__init_subclass__(**kwargs)
        if not register:
            return

        # Register the new interactors into the artifactclass
        if ArtifactInteractor._interactors.get(interactor_name):
            raise Errors.E020(name=interactor_name)  # type: ignore

        ArtifactInteractor._interactors[interactor_name] = cls

        get_module_logger(__name__).debug(f"registering '{interactor_name}' interactor")

    def _dispatch(self, callable: Callable, **kwargs) -> Dict[str, Any]:
        """
        Dispatch the variadic arguments the callable.

        Args:
            callable (Callable): The callable to dispatch arguments to.
        """

        sng = signature(callable)

        # If the callable accepts variadic keywords args, send them all
        has_variadics = any(p for p in sng.parameters.values() if p.kind == Parameter.VAR_KEYWORD)
        if has_variadics:
            return kwargs

        # Filter out non used arguments
        valids = set((p.name for p in sng.parameters.values()))
        args = {k: v for k, v in kwargs.items() if k in valids}

        return args

    @classmethod
    def interactors(cls):
        return cls._interactors

    @abstractmethod
    def load(self, **kwargs) -> Any:
        """
        Return the underlying asset.
        """

        raise NotImplementedError("must be implemented in the concrete class")

    @abstractmethod
    def save(self, asset: Any, **kwargs):
        """
        Save the underlying asset.
        """

        raise NotImplementedError("must be implemented in the concrete class")


class FileBasedInteractor(ArtifactInteractor, interactor_name="", register=False, metaclass=ABCMeta):
    """
    Extend the Artifact Interactor with Path interpolations
    """

    @dataclass
    class Extra:
        path: str  # Only the path is required for a FileBaseInteractor

    def __init__(self, artifact, *args, session: BaseSession = None, **kwargs):
        """
        Set the fragment from the Artifact Path
        """

        super().__init__(artifact, *args, session=session, **kwargs)  # type: ignore

        path = artifact.extra.path
        # Extract the fragment from the URI
        try:
            URI = self._interpolate_string(string=path, **kwargs)
            fragment = urlparse(URI)
        except BaseException as error:
            raise Errors.E0281(name=self.name) from error  # type: ignore

        self._fragment = fragment

    def _put(self, payload: bytes, **kwargs):
        """
        Put the payload to the URI

        Args:
            payload (BytesIO): The payload encoded as BytesIO to push.
        """

        # Fetch the class to use for the backend
        backend = Backend.backends().get(self._fragment.scheme, None)
        if not backend:
            raise Errors.E0292(scheme=fragment.scheme)  # type: ignore

        # Instanciate the bakcend
        backend = backend(session=self._session)

        # Combine the load options with the variadics ones
        options = {**self._save_options, **kwargs}
        options = self._dispatch(backend.put, **options)

        backend.put(payload=payload, fragment=self._fragment, **options)

    def _get(self, **kwargs) -> bytes:
        """
        Get a payload from the URI
        """

        # Fetch the backend'type to instanciate
        backend = Backend.backends().get(self._fragment.scheme, None)
        if not backend:
            raise Errors.E0292(scheme=self._fragment.scheme)  # type: ignore

        # Instanciate the bakcend
        backend = backend(session=self._session)

        # Combine the load options with the variadics ones
        options = {**self._load_options, **kwargs}
        options = self._dispatch(backend.get, **options)

        return backend.get(fragment=self._fragment, **options)  # type: ignore


class CSVInteractor(FileBasedInteractor, interactor_name="csv"):
    """
    Concrete implementation of a csv interactor
    """

    def __init__(self, artifact, *args, session: BaseSession = None, **kwargs):
        """
        Instanciate an interactor on a local file csv

        Args:
            path (Path): the path to load the file from.
            kwargs: named-arguments to be fowarded to the pandas.read_csv method.
        """

        super().__init__(artifact, *args, session=session, **kwargs)  # type: ignore

    def load(self, **kwargs) -> pd.DataFrame:
        """
        Parse 'path' as a pandas dataframe and return it

        Returns:
            pd.DataFrame: the parsed dataframe
        """

        self.debug(f"loading 'csv' : {self.name}")

        payload = self._get(**kwargs)

        # Combine the load options with the variadics ones
        options = {**self._load_options, **kwargs}
        options = self._dispatch(pd.read_csv, **options)

        try:
            df = pd.read_csv(BytesIO(payload), **options)
        except BaseException as err:
            raise Errors.E021(method="csv", path=self._path) from err  # type: ignore

        return df  # type: ignore

    def save(self, asset: Union[pd.DataFrame, pd.Series], **kwargs):
        """
        Save the 'data' dataframe as csv.

        Args:
            data (pandas.DataFrame): the dataframe to be saved
        """

        self.debug(f"saving 'csv' : {self.name}")

        if not isinstance(asset, (pd.DataFrame, pd.Series)):
            raise Errors.E023(
                interactor="csv",
                accept="pd.DataFrame, pd.Series",
                got=type(asset),
            )  # type: ignore

        # Combine the save options with the variadics ones
        options = {**self._save_options, **kwargs}
        options = self._dispatch(asset.to_csv, **options)

        try:
            payload = asset.to_csv(**options).encode("utf-8")  # type: ignore
            self._put(payload=payload, **kwargs)
        except BaseException as err:
            raise Errors.E022(method="csv", name=self.name) from err  # type: ignore


class XLSXInteractor(FileBasedInteractor, interactor_name="xslx"):
    """
    Concrete implementation of an XLSX interactor
    """

    def __init__(self, artifact, *args, session: BaseSession = None, **kwargs):
        """
        Instanciate an interactor on a local file xslsx

        Args:
            artifact (Artifact): the artifact to load
            kwargs: named-arguments.
        """

        super().__init__(artifact, *args, session=session, **kwargs)

    def load(self, **kwargs) -> pd.DataFrame:
        """
        Parse 'path' as a pandas dataframe and return it

        Returns:
            pd.DataFrame: the parsed dataframe
        """

        self.debug(f"loading 'xslx' : {self.name}")

        paylaod = self._get(**kwargs)

        # Combine the load options with the variadics ones
        options = {**self._load_options, **kwargs}
        options = self._dispatch(pd.read_excel, **options)

        try:
            df = pd.read_excel(paylaod, **options)  # type: ignore
        except BaseException as err:
            raise Errors.E021(method="xslx", path=self._path) from err  # type: ignore

        return df

    def save(self, asset: Union[pd.DataFrame, pd.Series], **kwargs):
        """
        Save the 'data' dataframe as csv.

        Args:
            data (pandas.DataFrame): the dataframe to be saved
        """

        self.debug(f"saving 'xslx' : {self.name}")

        if not isinstance(asset, (pd.DataFrame, pd.Series)):
            raise Errors.E023(
                interactor="xlsx",
                accept="pd.DataFrame, pd.Series",
                got=type(asset),
            )  # type: ignore

        # Combine the save options with the variadics ones
        options = {**self._save_options, **kwargs}
        options = self._dispatch(asset.to_excel, **options)

        try:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer) as writer:
                asset.to_excel(writer, **options)
            self._put(payload=buffer.getbuffer(), **kwargs)
        except BaseException as err:
            raise Errors.E022(method="xslx", name=self.name) from err  # type: ignore


class PicklerInteractor(FileBasedInteractor, interactor_name="pickle"):
    """
    Concrete implementation of a Pickle interactor.
    """

    def __init__(self, artifact, *args, session: BaseSession = None, **kwargs):
        """
        Instanciate an interactor on a local file pickle file

        Args:
            artifact (Artifact): the artifact to load
            kwargs: named-arguments.
        """

        super().__init__(artifact, *args, session=session, **kwargs)

    def load(self, **kwargs) -> Any:
        """
        Unserialize the object located at 'path'

        Returns:
            Any: the unpickled object
        """

        self.debug(f"loading 'pickle' : {self.name}")

        payload = self._get(**kwargs)

        # Combine the load options with the variadics ones
        options = {**self._load_options, **kwargs}
        options = self._dispatch(pickle.load, **options)

        try:
            obj = pickle.loads(payload, **options)
        except BaseException as err:
            raise Errors.E021(method="pickle", path=self.path) from err  # type: ignore

        return obj

    def save(self, asset: Any, **kwargs):
        """
        Serialize the 'asset'

        Args:
            asset (Any ): the artifact to be saved
        """

        self.debug(f"saving 'pickle' : {self.name}")

        # Combine the save options with the variadics ones
        options = {**self._save_options, **kwargs}
        options = self._dispatch(pickle.dumps, **options)

        try:
            payload = pickle.dumps(asset, **options)
            self._put(payload=payload, **kwargs)
        except BaseException as err:
            raise Errors.E022(method="pickle", name=self.name) from err  # type: ignore


# ------------------------------------------------------------------------- #


class ODBCInteractor(ArtifactInteractor, MixinInterpolable, interactor_name="odbc"):
    """
    Concrete implementation of an odbc interactor

    TODO : inject the odbc flavor (transac, athena, posgres...)
    TODO : implements the saving strategy
    """

    @dataclass
    class Extra:
        connection_string: str
        query: str

    def __init__(self, artifact, *args, session: BaseSession = None, **kwargs):
        """
        Instanciate an interactor against an odbc

        Args:
            query (str): The query to use to get the data
        """

        super().__init__(artifact, *args, session=session, **kwargs)  # type: ignore

        self._query = self._interpolate_string(artifact.extra.query, **kwargs)
        self._connection_string = artifact.extra.connection_string
        self._kwargs = kwargs

    @contextmanager
    def _get_connection(self):
        """
        Parse the connector and return the connection objecté
        """
        self.debug(f"requesting database connection.")

        try:
            with pyodbc.connect(self._connection_string) as cnxn:
                yield cnxn
        except BaseException as error:
            raise Errors.E025(dsn=self._connection_string) from error  # type: ignore

        self.debug(f"closing connection.")
        return

    def load(self, **kwargs) -> pd.DataFrame:
        """
        Parse 'path' as a pandas dataframe and return it

        Returns:
            pd.DataFrame: the parsed dataframe
        """
        self.debug(f"loading 'odbc' connection")

        # Combine the save options with the variadics ones
        options = {**self._load_options, **kwargs}
        options = self._dispatch(pd.read_sql, **options)

        data = None
        with self._get_connection() as cnxn:
            try:
                data = pd.read_sql(self._query, cnxn, **options)
            except BaseException as error:
                raise Errors.E026(query=self._query) from error  # type: ignore

        return data

    def save(self, asset: Any, **kwargs):
        raise Errors.E999()  # type: ignore


# ------------------------------------------------------------------------- #


class DatapaneInteractor(FileBasedInteractor, interactor_name="datapane"):
    """
    Implements saving / loading for datapane object.
    """

    def __init__(self, artifact, *args, session: BaseSession = None, **kwargs):
        """
        Return a new Datapane Interactor initiated with a a particular interactor
        """

        super().__init__(artifact, *args, session=session, **kwargs)

    def load(self, **kwargs):
        """
        Not implemented since I don't want a report to be altered as for now­

        Raises:
            NotImplementedErrord
        """

        raise Errors.E999()  # type: ignore

    def save(self, asset, **kwargs):
        """
        Save a datapane assert

        Args:
            artifact (Report): the datapane report object to be saved.
            open (bool): whether open the report on saving.

        Implementation details:
        * The datapane file is first written to temp directory before being serialized back to bytes (I failled lamentably at finding how to extract the HTML from datapane)
        """

        self.debug(f"saving 'datapane' : {self.name}")

        # Combine the load options with the variadics ones
        options = {**self._load_options, **kwargs}
        options = self._dispatch(asset.save, **options)

        try:

            with tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "report.html"
                asset.save(str(path), open=False, **options)

                with open(path, "rb") as f:
                    payload = f.read()
                    self._put(payload, **self._load_options, **kwargs)

        except BaseException as error:
            raise Errors.E022(method="datapane", name=self.name) from error  # type: ignore


# ------------------------------------------------------------------------- #


class BinaryInteractor(FileBasedInteractor, interactor_name="binary"):
    """
    Implements saving / loading for binary raw object
    """

    def __init__(self, artifact, *args, session: BaseSession = None, **kwargs):
        """
        Return a new Binary Interactor initiated with a a particular interactor
        """

        super().__init__(artifact, *args, session=session, **kwargs)

    def load(self, **kwargs):
        """
        Return the content of a binary artifact.
        """

        self.debug(f"loading 'binary' : {self.name}")

        payload = self._get(**kwargs)

        # Combine the load options with the variadics ones
        options = {**self._load_options, **kwargs}
        options = self._dispatch(BytesIO.read, **options)

        try:
            obj = BytesIO(payload).read(**options)
        except BaseException as err:
            raise Errors.E021(method="binary", name=self.name) from err  # type: ignore

        return obj

    def save(self, asset: bytes, **kwargs):
        """
        Save a datapane assert

        Args:
            artifact (Any): the binary content to write
        """

        self.debug(f"saving 'binary' : {self.name}")

        try:
            self._put(payload=asset, **kwargs)
        except BaseException as error:
            raise Errors.E022(method="binary", name=self.name) from error  # type: ignore


class FeatherInteractor(FileBasedInteractor, interactor_name="feather"):
    """
    Implements saving / loading for feather serialized object.
    Please : read https://arrow.apache.org/docs/python/feather.html to get a grasp of the Feather format.

    """

    def __init__(self, artifact, *args, session: BaseSession = None, **kwargs):
        """
        Return a new Feather Interactor.
        """

        super().__init__(artifact, *args, session=session, **kwargs)

    def load(self, **kwargs):
        """
        Return the content of a feather artifact.
        """

        self.debug(f"loading 'feather' : {self.name}")

        payload = self._get(**kwargs)

        # Combine the load options with the variadics ones
        options = {**self._load_options, **kwargs}
        options = self._dispatch(feather.read_feather, **options)

        try:
            obj = feather.read_feather(BytesIO(payload), **options)
        except BaseException as err:
            raise Errors.E021(method="feather", name=self.name) from err  # type: ignore

        return obj

    def save(self, asset: Union[pd.DataFrame, pd.Series], **kwargs):
        """
        Save a Feather asset

        Args:
            artifact Union[pd.DataFrame, pd.Series]: the dataframe content to write
        """

        self.debug(f"saving 'feather' : {self.name}")

        # Combine the save options with the variadics ones
        options = {**self._save_options, **kwargs}
        options = self._dispatch(feather.write_feather, **options)

        buffer = BytesIO()

        try:
            feather.write_feather(asset, buffer, **options)
            self._put(payload=buffer.getvalue(), **kwargs)
        except BaseException as error:
            raise Errors.E022(method="feather", name=self.name) from error  # type: ignore


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("artefact_interactor.py can't be run in standalone")
