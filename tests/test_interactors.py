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
# test_interactors.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Test the ability of the interactors to write / read objects
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

from pathlib import Path

import pytest
import pandas as pd

from statisfactory import Catalog, Session
from statisfactory.IO import Backend

#############################################################################
#                                  Script                                   #
#############################################################################


@pytest.fixture
def sess():
    """
    Create a Stati session
    """

    p = str(Path("tests/test_repo/").absolute())
    sess = Session(root_folder=p)

    return sess


@pytest.fixture
def catalog(sess) -> Catalog:
    """
    Create the Catalog

    Args:
        sess (Session): the Session to use to extract the catalogue from

    Returns:
        Catalog: The catalog of the test repo
    """

    return sess.catalog


def test_read_csv(catalog: Catalog):
    """
    Test the read of a CSV
    """

    out = catalog.load("test_read_csv")


def test_read_csv_load_options(catalog: Catalog):
    """
    Test the read of a CSV with a specified option
    """

    out = catalog.load("test_read_csv_options")
    assert out.index.name == "c"


def test_save_csv_save_options(catalog: Catalog):
    """
    Test the saving of the CSV and the support for options
    """

    out = catalog.load("test_read_csv_options")
    catalog.save("test_read_save_csv_options", out)

    out = catalog.load("test_read_save_csv_options")
    assert out.columns.to_list() == ["a", "b"]  # type: ignore


def test_priority_of_options(catalog: Catalog):
    """
    Provided args schould have an higher priority over thoose declared in the Catalog
    """

    out = catalog.load("test_read_csv_options", index_col=1)
    assert out.index.name == "b"


def test_read_xlsx_load_options(catalog: Catalog):
    """
    Test the read of a CSV
    """

    out = catalog.load("test_read_save_xlsx_options")


def test_read_pickle(catalog: Catalog):
    """
    Test the read of a CSV
    """

    out = catalog.load("test_read_read_pickle")


def test_read_pickle_ignore(catalog: Catalog):
    """
    Test the read of a Pickle while ignoring a parameter.
    """

    out = catalog.load("test_read_read_pickle", foobar=2)


def test_save_pickle_ignore(catalog: Catalog):
    """
    Test the saving of a Pickle while ignoring a parameter.
    """

    out = catalog.load("test_read_read_pickle", foobar=2)
    catalog.save("test_read_read_pickle", out, foobar=22)


@pytest.fixture
def custom_sess():
    """
    Create a Stati session
    """

    # Define a testable backend
    class TestableBackend(Backend, prefix="test"):
        """
        Test the implementation of a custom, testable backend.

        The Backend mutate two flags to notify the client of it's execution.
        """

        def __init__(self, session):
            super().__init__(session=session)

        def put(self, *, payload, fragment, flag_holder):
            flag_holder[0] = True

        def get(self, *, fragment, flag_holder):
            flag_holder[1] = True
            return b"1,2,3"

    p = str(Path("tests/test_repo/").absolute())
    sess = Session(root_folder=p)

    return sess


def test_read_with_backend(custom_sess):
    """
    Test the utilisation of a custom backend
    """

    flag_holder = [False, False]
    out = custom_sess.catalog.load("test_custom_backend", flag_holder=flag_holder)
    custom_sess.catalog.save("test_custom_backend", out, flag_holder=flag_holder)

    assert flag_holder[1]
    assert flag_holder[0]


def test_feather_serialisation(catalog: Catalog):
    """
    Test the write of a feather format
    """

    df = pd.DataFrame({"a": [1, 2]})

    # Serialize to feather
    catalog.save(name="test_feather", asset=df)

    # Retrieve from feather
    out = catalog.load("test_feather")

    assert df.equals(out)
