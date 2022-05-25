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
# test_catalog.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Test the catalogue
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

from pathlib import Path

import pytest

from statisfactory import Session, Catalog
from statisfactory import Artifact

#############################################################################
#                                 Packages                                  #
#############################################################################


@pytest.fixture
def sess(request):
    """
    Prepare stati session
    """

    p = str(Path(f"tests/{request.param}/").absolute())
    sess = Session(root_folder=p)
    return sess


@pytest.mark.parametrize(
    "sess",
    [
        "test_repo_multiple",
    ],
    indirect=True,
)
def test_check_existing_artifact(sess):

    target = Artifact(
        name="test_read_csv",
        type="csv",
        extra={"path": "tests/test_repo/data/test_read_csv.csv"},
        save_options={},
        load_options={},
    )

    artifact = sess.catalog._get_artifact("test_read_csv")
    assert target == artifact


@pytest.mark.parametrize(
    "sess",
    [
        "test_repo_multiple",
    ],
    indirect=True,
)
def test_multiple_catalogs(sess):

    target = Artifact(
        name="test_read_csv_2",
        type="csv",
        extra={"path": "tests/test_repo/data/test_read_csv.csv"},
        save_options={},
        load_options={},
    )

    artifact = sess.catalog._get_artifact("test_read_csv_2")
    assert target == artifact


@pytest.mark.parametrize(
    "sess",
    [
        "test_repo_multiple",
    ],
    indirect=True,
)
def test_deeply_catalogs(sess):

    sess.catalog._get_artifact("deeply_nested")


@pytest.mark.parametrize(
    "sess",
    [
        "test_repo_catalog_jinja",
    ],
    indirect=True,
)
def test_jinja_interpolation(sess):

    target = Artifact(
        name="dummy_artifact",
        type="csv",
        extra={"path": "tests/inteprolated/10_raw/!{dynamic}/test_read_csv.csv"},
        save_options={},
        load_options={},
    )

    artifact = sess.catalog._get_artifact("dummy_artifact")

    assert target == artifact


@pytest.mark.parametrize(
    "sess",
    [
        "test_repo_catalog_jinja",
    ],
    indirect=True,
)
def test_jinja_loop(sess):
    """
    Make sure the Jinja Loop is rendered
    """

    for item in ["foo", "bar", "spam"]:
        sess.catalog._get_artifact(f"{item}_artifact")
