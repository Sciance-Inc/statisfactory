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
from statisfactory.IO.artifacts import artifact_interactor

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


@pytest.mark.parametrize(
    "sess",
    [
        "test_repo",
    ],
    indirect=True,
)
def test_odbc_interactor(sess):
    """
    Make sure None port are properly handled
    """

    artifact_repr = sess.catalog._get_artifact("test_odbc")
    interactor_factory = sess.catalog._get_interactor(artifact_repr)

    args = interactor_factory(artifact_repr, sess=sess)._connection_url.translate_connect_args()

    assert args == {"host": "192.19.1.1", "database": "test", "username": "foobar", "password": "spam"}


@pytest.mark.parametrize(
    "sess",
    [
        "test_repo",
    ],
    indirect=True,
)
def test_odbc_interactor_dynamic(sess):
    """
    Make sure None port are properly handled
    """

    artifact_repr = sess.catalog._get_artifact("test_odbc_dynamic")
    interactor_factory = sess.catalog._get_interactor(artifact_repr)

    args = interactor_factory(artifact_repr, sess=sess, port=1234)._connection_url.translate_connect_args()
    assert args == {"host": "192.19.1.1", "database": "test", "username": "foobar", "password": "spam", "port": 1234}

    args = interactor_factory(artifact_repr, sess=sess, port=None)._connection_url.translate_connect_args()
    assert args == {"host": "192.19.1.1", "database": "test", "username": "foobar", "password": "spam"}


@pytest.mark.parametrize(
    "sess",
    [
        "test_repo",
    ],
    indirect=True,
)
def test_odbc_eval_mapper_selector(sess):
    """
    Check the chaining on static > dynamic interpolation
    """
    artifact_repr = sess.catalog._get_artifact("test_eval_interpolation_selector")
    interactor_factory = sess.catalog._get_interactor(artifact_repr)
    port_mapping = {"base": 1234, "alternate": 5678}
    args = interactor_factory(artifact_repr, sess=sess, port_mapping=port_mapping)._connection_url.translate_connect_args()
    assert args == {"host": "192.19.1.1", "database": "test", "username": "foobar", "password": "spam", "port": 1234}


@pytest.mark.parametrize(
    "sess",
    [
        "test_repo",
    ],
    indirect=True,
)
def test_odbc_eval_selector_mapper(sess):
    """
    Check the chaining on static > dynamic interpolation
    """
    artifact_repr = sess.catalog._get_artifact("test_eval_interpolation_mapper")
    interactor_factory = sess.catalog._get_interactor(artifact_repr)
    args = interactor_factory(artifact_repr, sess=sess, selector="base")._connection_url.translate_connect_args()
    assert args == {"host": "192.19.1.1", "database": "test", "username": "foobar", "password": "spam", "port": 1234}


@pytest.mark.parametrize(
    "sess",
    [
        "test_repo",
    ],
    indirect=True,
)
def test_odbc_eval_selector_mapper_null(sess):
    """
    Check the chaining on static > dynamic interpolation
    """
    artifact_repr = sess.catalog._get_artifact("test_eval_interpolation_mapper_null")
    interactor_factory = sess.catalog._get_interactor(artifact_repr)
    args = interactor_factory(artifact_repr, sess=sess, selector="base")._connection_url.translate_connect_args()
    assert args == {"host": "192.19.1.1", "database": "test", "username": "foobar", "password": "spam"}
