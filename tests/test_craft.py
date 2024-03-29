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
# test_craft.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Test the Craft arguments mechanisme
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

from pathlib import Path

import pandas as pd
import pytest

from statisfactory import Artifact, Craft, Session, Volatile
from statisfactory.IO import Backend
from statisfactory.operator import craft

#############################################################################
#                                 Packages                                  #
#############################################################################


@pytest.fixture
def sess():
    """
    Create a Stati session
    """

    p = str(Path("tests/test_repo/").absolute())
    sess = Session(root_folder=p)

    return sess


def test_craft_no_args(sess):
    """
    Test the execution of a craft without arguments
    """

    @Craft()
    def step_1() -> Volatile("out_1"):  # type: ignore
        return 1

    with sess:
        out = step_1()

    assert out == 1


def test_craft_keyword_args(sess):
    """
    Test the execution of a craft with a keyword arg
    """

    @Craft()
    def step_1(val) -> Volatile("out_1"):  # type: ignore
        return val

    with sess:
        out = step_1(val=3)

    assert out == 3


def test_craft_default_keyword_args(sess):
    """
    Test the execution of a craft with a default keyword arg
    """

    @Craft()
    def step_1(val=5) -> Volatile("out_1"):  # type: ignore
        return val

    with sess:
        out = step_1()

    assert out == 5


def test_craft_default_keyword_precedence_args(sess):
    """
    Test the execution of a craft with a default keyword arg and a provided value
    """

    @Craft()
    def step_1(val=5) -> Volatile("out_1"):  # type: ignore
        return val

    with sess:
        out = step_1(val=7)

    assert out == 7


def test_variadic_artifact_interpolation_dispatching(sess):
    """
    Check if the variadic arguments are correctly dispatched for interpolations
    """

    @Craft()
    def spam(test_read_variadic_pickle: Artifact) -> Volatile("step_1"):  # type: ignore
        ...

    with sess:
        spam(variadic="data")


@pytest.fixture
def custom_sess():
    """
    Create a Stati session with a testable backend
    """

    # Define a testable backend
    class TestableBackend(Backend, prefix="testargs"):
        """
        Test the implementation of a custom, testable backend.

        The Backend mutate two flags to notify the client of it's execution.
        """

        def __init__(self, session):
            super().__init__(session=session)

        def put(self, *, payload, fragment, args_holder, **kwargs):
            args_holder[0] = kwargs

        def get(self, *, fragment, args_holder, **kwargs):
            args_holder[1] = kwargs
            return b"1,2,3"

    p = str(Path("tests/test_repo/").absolute())
    sess = Session(root_folder=p)

    return sess


def test_propagating_configuration_to_backend(custom_sess):
    """
    Check that Craft's argument are correcly cascaded to a backend
    """

    @Craft()
    def foo(test_custom_backend_args: Artifact):
        ...

    @Craft()
    def spam() -> Artifact("test_custom_backend_args"):  # type: ignore
        return pd.DataFrame()

    args_holder = [None, None]

    # Test the get
    with custom_sess:
        foo(spam=1, args_holder=args_holder)

    # Test the put
    with custom_sess:
        spam(spam=1, args_holder=args_holder)

    assert args_holder == [{"spam": 1}, {"spam": 1}]


def test_key_variadic_argument(sess):
    @Craft()
    def foo(**kwargs) -> Volatile("out_1"):
        return kwargs

    @Craft()
    def bar(out_1: Volatile) -> Volatile("out_2"):
        return out_1["spam"]

    with sess:
        out = (foo + bar)(spam=1)


    assert out["out_2"] == 1
