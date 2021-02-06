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

# project
from .errors import errors
from .models import Connector, Artefact
from .logger import MixinLogable

# third party
import pandas as pd

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
    def __init__(self, *args, **kwargs):
        """
        Instanciate a new interactor.

        The interactors implements cooperative inheritance, only the artefact is mandatory.
        """

        super().__init__(*args, **kwargs)

    @abstractmethod
    def load(self) -> pd.DataFrame:
        """
        Return the underlying concrete dataset as a pandas dataframe

        Returns:
            pd.DataFrame: [description]
        """

        raise NotImplementedError("must be implemented in the concrete class")

    @abstractmethod
    def save(self, df: pd.DataFrame):
        """
        Saved the underlying

        Returns:
            pd.DataFrame: [description]
        """

        raise NotImplementedError("must be implemented in the concrete class")


# ------------------------------------------------------------------------- #


class MixinLocalFileSystem:
    """
    Implements helpers to manipulate a local file system.
    """

    def __init__(self, *args, **kwargs):
        """
        Instanciate a MixinLocalFileSystem. For cooperative multiple inheritance only.
        """

        super().__init__(*args, **kwargs)

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

        path = data_path / Path(path.format(**kwargs))

        return path.absolute()


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
            df = pd.read_csv(self._path)
        except FileNotFoundError as err:
            raise errors.E024(__name__, path=self._path) from err
        except BaseException as err:
            raise errors.E021(__name__, method="csv", path=self._path) from err

        return df

    # TODO : controle contravariance
    def save(self, df: pd.DataFrame):
        """
        Save the 'data' dataframe as csv.
        Args:
            data (pandas.DataFrame): the dataframe to be saved
        """

        self.debug(f"saving 'csv' : {self._path}")

        try:
            df.to_csv(self._path)
        except BaseException as err:
            raise errors.E022(__name__, path=self._path) from err


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
            df = pd.read_excel(self._path)
        except FileNotFoundError as err:
            raise errors.E024(__name__, path=self._path) from err
        except BaseException as err:
            raise errors.E021(__name__, method="xslx", path=self._path) from err

        return df

    # TODO : controle contravariance
    def save(self, df: pd.DataFrame):
        """
        Save the 'data' dataframe as csv.
        Args:
            data (pandas.DataFrame): the dataframe to be saved
        """

        self.debug(f"loading 'xslx' : {self._path}")

        try:
            df.to_excel(self._path)
        except BaseException as err:
            raise errors.E022(__name__, path=self._path) from err


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

    def load(self) -> pd.DataFrame:
        """
        Parse 'path' as a pandas dataframe and return it

        Returns:
            pd.DataFrame: the parsed dataframe
        """
        self.debug(f"loading 'odbc' : {self._path}")

        raise BaseException("Will be implemented in few more beers !")

    def save(self, df: pd.DataFrame):
        raise errors.E999


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("interface.py can't be run in standalone")
