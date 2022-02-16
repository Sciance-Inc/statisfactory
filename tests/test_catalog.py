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
        path="tests/test_repo/data/test_read_csv.csv",
        connector=None,
        query=None,
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
        path="tests/test_repo/data/test_read_csv.csv",
        connector=None,
        query=None,
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
        path="tests/inteprolated/10_raw/!{dynamic}/test_read_csv.csv",
        connector=None,
        query=None,
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
