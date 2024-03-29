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
# test_models.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Test the Models and the Models building mechanisme
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

from pathlib import Path
import pytest

from statisfactory import Artifact, Craft, Session, Volatile

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


def test_building_chains_of_volatiles(sess):
    """
    Test than an iterable if volatiles can be build using the 'of' method
    """

    A, B = Volatile.of("A", "B")

    assert isinstance(A, Volatile) and A.name == "A"
    assert isinstance(B, Volatile) and B.name == "B"


def test_building_chains_of_artifacts(sess):
    """
    Test than an iterable if artifacts can be build using the 'of' method
    """

    A, B = Artifact.of("A", "B")

    assert isinstance(A, Artifact) and A.name == "A"
    assert isinstance(B, Artifact) and B.name == "B"


def test_chaining_from_crafts(sess):
    """
    Tests that artifacts and volatilses formed by of can be used in the Craft's definition
    """

    @Craft()
    def foo() -> Volatile.of("A", "B"):
        return 1, 2

    with sess:
        a, b = foo()

    assert a == 1
    assert b == 2


def test_chaining_in_pipeline(sess):
    """
    Tests that volatiles might be consistenly used in a pipeline definitio and that intercraft dependencies are correctly solved
    """

    @Craft()
    def foo() -> Volatile.of("A", "B"):
        return 1, 2

    @Craft()
    def spam(A: Volatile, B: Volatile) -> Volatile("c"):
        return A + B

    with sess:
        out = (spam + foo)()

    assert out["c"] == 3


def test_combining_of_and_explicit(sess):
    """
    Check that using the of syntax still supports explicit Volatile assignement
    """

    @Craft()
    def foo() -> Volatile.of("a", "b") + [Volatile("c")]:
        return 1, 2, 3

    with sess:
        *_, c = foo()

    assert c == 3


def test_combining_multiples_of(sess):
    """
    Check that using the of syntax still supports adding two off generated lists
    """

    @Craft()
    def foo() -> Volatile.of("a", "b") + Volatile.of("c"):
        return 1, 2, 3

    with sess:
        *_, c = foo()

    assert c == 3
