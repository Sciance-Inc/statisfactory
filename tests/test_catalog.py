#! /usr/bin/python3

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

from statisfactory import Session
from statisfactory import Artefact

#############################################################################
#                                 Packages                                  #
#############################################################################


def test_check_existing_artifact():

    p = str(Path("tests/test_repo/").absolute())
    sess = Session(root_folder=p)

    target = Artefact(
        name="test_read_csv",
        type="csv",
        path="tests/test_repo/data/test_read_csv.csv",
        connector=None,
        query=None,
        save_options={},
        load_options={},
    )

    artefact = sess.catalog._get_artefact("test_read_csv")
    assert target == artefact


def test_multiple_catalogs():

    p = str(Path("tests/test_repo_multiple/").absolute())
    sess = Session(root_folder=p)

    target = Artefact(
        name="test_read_csv_2",
        type="csv",
        path="tests/test_repo/data/test_read_csv.csv",
        connector=None,
        query=None,
        save_options={},
        load_options={},
    )

    artefact = sess.catalog._get_artefact("test_read_csv_2")
    assert target == artefact


def test_deeply_catalogs():

    p = str(Path("tests/test_repo_multiple/").absolute())
    sess = Session(root_folder=p)

    sess.catalog._get_artefact("deeply_nested")
