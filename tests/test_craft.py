#! /usr/bin/python3

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

from statisfactory import Session, Craft, Volatile
import pytest
from pathlib import Path

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
