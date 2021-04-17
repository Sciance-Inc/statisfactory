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
from string import Template

# project
from .models import _ArtefactSchema
from ..errors import errors
from ..logger import get_module_logger, MixinLogable


# third party
import pandas as pd
import pyodbc
import datapane as dp

#############################################################################
#                                  Script                                   #
#############################################################################


class DynamicInterpolation(Template):
    """
    Implements the interpolation of the !{} for the artefact values.
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
    """


class ArtefactInteractor(MixinLogable, metaclass=ABCMeta):
    """
    An interactor wraps the loading and the saving of a dataframe.
    A dataset is loaded as a pandas dataframe
    The dataset can only saved pandas dataframe

    TODO : v2 (oos) add support for batchs retrieval
    TODO : v2 (oos) add support for writting files (parceque csv en s3...)
    """

    # A placeholder for all registered interactors
    _interactors = dict()

    def __init_subclass__(cls, interactor_name, **kwargs):
        """
        Implement the registration of a child class into the artefact class.
        By doing so, the ArtefactInteractor can be extended to use new interactors without updating the code of the class (Open Close principle)
        See PEP-487 for details.
        """

        super().__init_subclass__(**kwargs)

        # Register the new interactors into the artefactclass
        if ArtefactInteractor._interactors.get(interactor_name):
            raise errors.E020(__name__, name=interactor_name)

        ArtefactInteractor._interactors[interactor_name] = cls

        # Propagate the change to the model validator
        _ArtefactSchema.valids_artefacts.add(interactor_name)

        get_module_logger("statisfactory").debug(
            f"registering '{interactor_name}' interactor"
        )

    @classmethod
    def interactors(cls):
        return cls._interactors

    @abstractmethod
    def __init__(self, artefact, *args, **kwargs):
        """
        Instanciate a new interactor.

        The interactors implements cooperative inheritance, only the artefact is mandatory.
        """

        super().__init__(__name__, *args, **kwargs)
        self.name = artefact.name
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


class MixinInterpolable:
    """
    Implements helpers to interpolate a string
    """

    def __init__(self, artefact, *args, **kwargs):
        """
        Instanciate the MixinInterpolable
        """

        super().__init__(artefact, *args, **kwargs)

    def _interpolate_string(self, string, **kwargs):
        """
        Interpolate a given string using the provided context
        """

        if not string:
            raise errors.E027(__name__)

        try:
            string = DynamicInterpolation(string).substitute(**kwargs)
        except KeyError as err:
            raise errors.E028(__name__, trg=string) from err

        return string


class MixinLocalFileSystem(MixinInterpolable):
    """
    Implements helpers to manipulate a local file system.
    """

    def __init__(self, artefact, *args, **kwargs):
        """
        Instanciate a MixinLocalFileSystem. For cooperative multiple inheritance only.
        """

        super().__init__(artefact, *args, **kwargs)

    def _interpolate_path(self, path: Union[str, Path], **kwargs) -> Path:
        """
        Interpolate the Path using a provided a context
        The string is interpolated the named variadics arguments.

        Returns:
            Path: the fully qualified canonical path to the artefact.
        """

        path = Path(self._interpolate_string(str(path), **kwargs))
        return path.absolute()

    def _create_parents(self, path: Path):
        """
        Create all the parents of a given path.

        Args:
            path (Path): the path to create the parent from
        """

        path.parents[0].mkdir(parents=True, exist_ok=True)


# ------------------------------------------------------------------------- #


class CSVInteractor(ArtefactInteractor, MixinLocalFileSystem, interactor_name="csv"):
    """
    Concrete implementation of a csv interactor
    """

    def __init__(self, artefact, *args, **kwargs):
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

        self.debug(f"loading 'csv' : {self.name}")

        try:
            df = pd.read_csv(self._path, **self._load_options)
        except FileNotFoundError as err:
            raise errors.E024(__name__, path=self._path) from err
        except BaseException as err:
            raise errors.E021(__name__, method="csv", path=self._path) from err

        return df

    def save(self, asset: Union[pd.DataFrame, pd.Series]):
        """
        Save the 'data' dataframe as csv.
        Args:
            data (pandas.DataFrame): the dataframe to be saved
        """

        self.debug(f"saving 'csv' : {self.name}")

        if not isinstance(asset, (pd.DataFrame, pd.Series)):
            raise errors.E023(
                __name__,
                interactor="csv",
                accept="pd.DataFrame, pd.Series",
                got=type(asset),
            )

        self._create_parents(self._path)

        try:
            asset.to_csv(self._path, **self._save_options)
        except BaseException as err:
            raise errors.E022(__name__, method="csv", name=self.name) from err


# ------------------------------------------------------------------------- #


class XLSXInteractor(ArtefactInteractor, MixinLocalFileSystem, interactor_name="xslx"):
    """
    Concrete implementation of an XLSX interactor
    """

    def __init__(self, artefact, *args, **kwargs):
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

        self.debug(f"loading 'xslx' : {self.name}")

        try:
            df = pd.read_excel(self._path, **self._load_options)
        except FileNotFoundError as err:
            raise errors.E024(__name__, path=self._path) from err
        except BaseException as err:
            raise errors.E021(__name__, method="xslx", path=self._path) from err

        return df

    def save(self, asset: Union[pd.DataFrame, pd.Series]):
        """
        Save the 'data' dataframe as csv.
        Args:
            data (pandas.DataFrame): the dataframe to be saved
        """

        self.debug(f"saving 'xslx' : {self.name}")

        if not isinstance(asset, (pd.DataFrame, pd.Series)):
            raise errors.E023(
                __name__,
                interactor="xlsx",
                accept="pd.DataFrame, pd.Series",
                got=type(asset),
            )

        self._create_parents(self._path)

        try:
            asset.to_excel(self._path, **self._save_options)
        except BaseException as err:
            raise errors.E022(__name__, method="xslx", name=self.name) from err


# ------------------------------------------------------------------------- #


class PicklerInteractor(
    ArtefactInteractor, MixinLocalFileSystem, interactor_name="pickle"
):
    """
    Concrete implementation of a Pickle interactor.
    """

    def __init__(self, artefact, *args, **kwargs):
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

        self.debug(f"loading 'pickle' : {self.name}")

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

        self.debug(f"saving 'pickle' : {self.name}")

        self._create_parents(self._path)

        try:
            with open(self._path, "wb+") as f:
                pickle.dump(asset, f, **self._save_options)
        except BaseException as err:
            raise errors.E022(__name__, method="pickle", name=self.name) from err


# ------------------------------------------------------------------------- #


class ODBCInteractor(ArtefactInteractor, MixinInterpolable, interactor_name="odbc"):
    """
    Concrete implementation of an odbc interactor

    TODO : inject the odbc flavor (transac, athena, posgres...)
    TODO : implements the saving strategy
    """

    def __init__(self, artefact, connector, *args, **kwargs):
        """
        Instanciate an interactor against an odbc

        Args:
            query (str): The query to use to get the data
            connector (Connector): [description]
        """

        self._query = self._interpolate_string(artefact.query, **kwargs)
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
            raise errors.E025(__name__, name=self._connector.name) from error

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
                raise errors.E026(
                    __name__, query=self._query, name=self._connector.name
                ) from error

        return data

    def save(self, asset: Any):
        raise errors.E999


# ------------------------------------------------------------------------- #


class DatapaneInteractor(
    ArtefactInteractor, MixinLocalFileSystem, interactor_name="datapane"
):
    """
    Implements saving / loading for datapane object.
    """

    def __init__(self, artefact, *args, **kwargs):
        """
        Return a new Datapane Interactor initiated with a a particular interactor
        """

        super().__init__(artefact, *args, **kwargs)
        self._path = self._interpolate_path(path=artefact.path, **kwargs)

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

        self.debug(f"saving 'datapane' : {self.name}")

        try:
            self._create_parents(self._path)
            asset.save(self._path, open=open, **self._load_options)
        except BaseException as error:
            raise errors.E022(__name__, method="datapane", name=self.name) from error


# ------------------------------------------------------------------------- #


class BinaryInteractor(
    ArtefactInteractor, MixinLocalFileSystem, interactor_name="binary"
):
    """
    Implements saving / loading for binary raw object
    """

    def __init__(self, artefact, *args, **kwargs):
        """
        Return a new Binary Interactor initiated with a a particular interactor
        """

        super().__init__(artefact, *args, **kwargs)
        self._path = self._interpolate_path(path=artefact.path, **kwargs)

    def load(self):
        """
        Return the content of a binary artefact.
        """

        self.debug(f"loading 'binary' : {self.name}")

        try:
            with open(self._path, "rb") as f:
                obj = f.read(**self._load_options)
        except FileNotFoundError as err:
            raise errors.E024(__name__, path=self._path) from err
        except BaseException as err:
            raise errors.E021(__name__, method="binary", name=self.name) from err

        return obj

    def save(self, asset: Any):
        """
        Save a datapane assert

        Args:
            artefact (Any): the binary content to write
        """

        self.debug(f"saving 'binary' : {self.name}")

        try:
            self._create_parents(self._path)
            with open(self._path, "wb+") as f:
                f.write(asset, **self._save_options)
        except BaseException as error:
            raise errors.E022(__name__, method="binary", name=self.name) from error


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("interface.py can't be run in standalone")
