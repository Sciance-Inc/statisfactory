#! /usr/bin/python3

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
from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import Any, Union
from contextlib import contextmanager
import pickle

# project
from .errors import errors
from .models import Connector, Artefact
from .logger import MixinLogable

# third party
import pandas as pd
import pyodbc
import datapane as dp

#############################################################################
#                                  Script                                   #
#############################################################################


class ArtefactInteractor(MixinLogable, metaclass=ABCMeta):
    """
    An interactor wraps the loading and the saving of a dataframe.
    A dataset is loaded as a pandas dataframe
    The dataset can only saved pandas dataframe

    TODO : v2 (oos) add support for batchs retrieval
    TODO : v2 (oos) add support for writting files (parceque csv en s3...)
    """

    @abstractmethod
    def __init__(self, artefact, *args, **kwargs):
        """
        Instanciate a new interactor.

        The interactors implements cooperative inheritance, only the artefact is mandatory.
        """

        super().__init__(artefact, *args, **kwargs)
        self._save_options = artefact.save_options
        self._load_options = artefact.load_options

    @abstractmethod
    def load(self) -> Any:
        """
        Return the underlying asset.
        """

        raise NotImplementedError("must be implemented in the concrete class")

    @abstractmethod
    def save(self, asset: Any):
        """
        Save the underlying asset.
        """

        raise NotImplementedError("must be implemented in the concrete class")


# ------------------------------------------------------------------------- #


class MixinLocalFileSystem:
    """
    Implements helpers to manipulate a local file system.

    TODO : make sure that there is not residual {} in the path
    """

    def __init__(self, artefact, *args, **kwargs):
        """
        Instanciate a MixinLocalFileSystem. For cooperative multiple inheritance only.
        """

        super().__init__(artefact, *args, **kwargs)

    def _interpolate_path(self, data_path: Path, path: str, **kwargs) -> Path:
        """
        Build a local path from minimally a root path and a path.
        The string is interpolated the named variadics arguments.

        Args:
            data_path (str): the root path (aka, the catalog root dir)
            path (str): the artefact path (relative from the catalog root dir)
            **kwargs : the varaibles to be interpolated in the path

        Returns:
            Path: the fully qualified canonical path to the artefact.
        """

        if data_path == Path("") or path == "":
            raise errors.E023(__name__, data_path=data_path, path=path)

        try:
            path = data_path / Path(path.format(**kwargs))
        except KeyError as err:
            raise errors.E026(__name__, path=path) from err

        return path.absolute()

    def _create_parents(self, path: Path):
        """
        Create all the parents of a given path.

        Args:
            path (Path): the path to create the parent from
        """

        path.parents[0].mkdir(parents=True, exist_ok=True)


# ------------------------------------------------------------------------- #


class CSVInteractor(ArtefactInteractor, MixinLocalFileSystem):
    """
    Concrete implementation of a csv interactor
    """

    def __init__(self, artefact: Artefact, *args, **kwargs):
        """
        Instanciate an interactor on a local file csv

        Args:
            path (Path): the path to load the file from.
            kwargs: named-arguments to be fowarded to the pandas.read_csv method.
        """

        super().__init__(artefact, *args, **kwargs)

        self._path = self._interpolate_path(path=artefact.path, **kwargs)
        self._kwargs = kwargs

    def load(self) -> pd.DataFrame:
        """
        Parse 'path' as a pandas dataframe and return it

        Returns:
            pd.DataFrame: the parsed dataframe
        """

        self.debug(f"loading 'csv' : {self._path}")

        try:
            df = pd.read_csv(self._path, **self._load_options)
        except FileNotFoundError as err:
            raise errors.E024(__name__, path=self._path) from err
        except BaseException as err:
            raise errors.E021(__name__, method="csv", path=self._path) from err

        return df

    # TODO : controle contravariance
    def save(self, asset: Union[pd.DataFrame, pd.Series]):
        """
        Save the 'data' dataframe as csv.
        Args:
            data (pandas.DataFrame): the dataframe to be saved
        """

        self.debug(f"saving 'csv' : {self._path}")

        if not isinstance(asset, (pd.DataFrame, pd.Series)):
            raise errors.E025(
                __name__,
                interactor="csv",
                accept="pd.DataFrame, pd.Series",
                got=type(asset),
            )

        self._create_parents(self._path)

        try:
            asset.to_csv(self._path, **self._save_options)
        except BaseException as err:
            raise errors.E022(__name__, method="csv", name=self._name) from err


# ------------------------------------------------------------------------- #


class XLSXInteractor(ArtefactInteractor, MixinLocalFileSystem):
    """
    Concrete implementation of an XLSX interactor
    """

    def __init__(self, artefact: Artefact, *args, **kwargs):
        """
        Instanciate an interactor on a local file xslsx

        Args:
            artefact (Artefact): the artefact to load
            kwargs: named-arguments.
        """

        super().__init__(artefact, *args, **kwargs)

        self._path = self._interpolate_path(path=artefact.path, **kwargs)
        self._kwargs = kwargs

    def load(self) -> pd.DataFrame:
        """
        Parse 'path' as a pandas dataframe and return it

        Returns:
            pd.DataFrame: the parsed dataframe

        TODO : add a wrapper for kwargs
        """

        self.debug(f"loading 'xslx' : {self._path}")

        try:
            df = pd.read_excel(self._path, **self._load_options)
        except FileNotFoundError as err:
            raise errors.E024(__name__, path=self._path) from err
        except BaseException as err:
            raise errors.E021(__name__, method="xslx", path=self._path) from err

        return df

    # TODO : controle contravariance
    def save(self, asset: Union[pd.DataFrame, pd.Series]):
        """
        Save the 'data' dataframe as csv.
        Args:
            data (pandas.DataFrame): the dataframe to be saved
        """

        self.debug(f"saving 'xslx' : {self._path}")

        if not isinstance(asset, (pd.DataFrame, pd.Series)):
            raise errors.E025(
                __name__,
                interactor="xlsx",
                accept="pd.DataFrame, pd.Series",
                got=type(asset),
            )

        self._create_parents(self._path)

        try:
            asset.to_excel(self._path, **self._save_options)
        except BaseException as err:
            raise errors.E022(__name__, method="xslx", name=self._name) from err


# ------------------------------------------------------------------------- #


class PicklerInteractor(ArtefactInteractor, MixinLocalFileSystem):
    """
    Concrete implementation of a Pickle interactor.
    """

    def __init__(self, artefact: Artefact, *args, **kwargs):
        """
        Instanciate an interactor on a local file pickle file

        Args:
            artefact (Artefact): the artefact to load
            kwargs: named-arguments.
        """

        super().__init__(artefact, *args, **kwargs)

        self._path = self._interpolate_path(path=artefact.path, **kwargs)
        self._kwargs = kwargs

    def load(self) -> Any:
        """
        Unserialize the object located at 'path'

        Returns:
            Any: the unpickled object

        TODO : add a wrapper for kwargs
        """

        self.debug(f"loading 'pickle' : {self._path}")

        try:
            with open(self._path, "rb") as f:
                obj = pickle.load(f, **self._load_options)
        except FileNotFoundError as err:
            raise errors.E024(__name__, path=self._path) from err
        except BaseException as err:
            raise errors.E021(__name__, method="pickle", path=self._path) from err

        return obj

    def save(self, asset: Any):
        """
        Serialize the 'asset'
        Args:
            asset (Any ): the artefact to be saved
        """

        self.debug(f"saving 'pickle' : {self._path}")

        self._create_parents(self._path)

        try:
            with open(self._path, "wb+") as f:
                pickle.dump(asset, f, **self._save_options)
        except BaseException as err:
            raise errors.E022(__name__, method="pickle", name=self._name) from err


# ------------------------------------------------------------------------- #


class ODBCInteractor(ArtefactInteractor):
    """
    Concrete implementation of an odbc interactor

    TODO : inject the odbc flavor (transac, athena, posgres...)
    TODO : implements the saving strategy
    """

    def __init__(self, artefact: Artefact, connector: Connector, *args, **kwargs):
        """
        Instanciate an interactor against an odbc

        Args:
            query (str): The query to use to get the data
            connector (Connector): [description]
        """

        self._query = artefact.query
        self._connector = connector
        self._kwargs = kwargs

        super().__init__(artefact, *args, **kwargs)

    @contextmanager
    def _get_connection(self):
        """
        Parse the connector and return the connection objecté

        Args:
            connector (Connector): the connector object to parse.

        TODO : add support for parameters
        """
        self.debug(f"requesting connection to {self._connector.name}")

        try:
            with pyodbc.connect(self._connector.connString) as cnxn:
                yield cnxn
        except BaseException as error:
            raise errors.E027(__name__, name=self._connector.name) from error

        self.debug(f"closing connection to {self._connector.name}")
        return

    def load(self) -> pd.DataFrame:
        """
        Parse 'path' as a pandas dataframe and return it

        Returns:
            pd.DataFrame: the parsed dataframe
        """
        self.debug(f"loading 'odbc' : {self._connector.name}")

        data = None
        with self._get_connection() as cnxn:
            try:
                data = pd.read_sql(self._query, cnxn, **self._load_options)
            except BaseException as error:
                raise errors.E028(
                    __name__, query=self._query, name=self._connector.name
                ) from error

        return data

    def save(self, asset: Any):
        raise errors.E999


# ------------------------------------------------------------------------- #


class DatapaneInteractor(ArtefactInteractor, MixinLocalFileSystem):
    """
    Implements saving / loading for datapane object.
    """

    def __init__(self, artefact: Artefact, *args, **kwargs):
        """
        Return a new Datapane Interactor initiated with a a particular interactor
        """

        super().__init__(artefact, *args, **kwargs)
        self._path = self._interpolate_path(path=artefact.path, **kwargs)
        self._name = artefact.name

    def load(self):
        """
        Not implemented since I don't want a report to be altered as for now­
        Raises:
            NotImplementedErrord
        """

        raise errors.E999

    def save(self, asset: dp.Report, open=False):
        """
        Save a datapane assert

        Args:
            artefact (dp.Report): the datapane report object to be saved.
            open (bool): whether open the report on saving.
        """

        self.debug(f"saving 'datapane' : {self._name}")

        try:
            self._create_parents(self._path)
            asset.save(self._path, open=open, **self._load_options)
        except BaseException as error:
            raise errors.E022(__name__, method="datapane", name=self._name) from error


# ------------------------------------------------------------------------- #


class BinaryInteractor(ArtefactInteractor, MixinLocalFileSystem):
    """
    Implements saving / loading for binary raw object
    """

    def __init__(self, artefact: Artefact, *args, **kwargs):
        """
        Return a new Binary Interactor initiated with a a particular interactor
        """

        super().__init__(artefact, *args, **kwargs)
        self._path = self._interpolate_path(path=artefact.path, **kwargs)
        self._name = artefact.name

    def load(self):
        """
        Return the content of a binary artefact.
        """

        self.debug(f"loading 'binary' : {self._path}")

        try:
            with open(self._path, "rb") as f:
                obj = f.read(**self._load_options)
        except FileNotFoundError as err:
            raise errors.E024(__name__, path=self._path) from err
        except BaseException as err:
            raise errors.E021(__name__, method="binary", path=self._path) from err

        return obj

    def save(self, asset: Any):
        """
        Save a datapane assert

        Args:
            artefact (Any): the binary content to write
        """

        self.debug(f"saving 'binary' : {self._name}")

        try:
            self._create_parents(self._path)
            with open(self._path, "wb+") as f:
                f.write(asset, **self._save_options)
        except BaseException as error:
            raise errors.E022(__name__, method="binary", name=self._name) from error


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("interface.py can't be run in standalone")
