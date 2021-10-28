#! /usr/bin/python3

# test_pipeline.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Test the ability of the pipeline to solve dependencies as well as the propagtion of arguments
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


def test_linear_pipeline_no_args(sess):
    """
    Test the execution of a linear pipeline without any configuration
    """

    @Craft()
    def step_1() -> Volatile("out_1"):  # type: ignore
        return 1

    @Craft()
    def step_2(out_1: Volatile) -> Volatile("out_2"):  # type: ignore
        return 2

    @Craft()
    def step_3(out_2: Volatile) -> Volatile("out_3"):  # type: ignore
        return 3

    p = step_1 + step_2 + step_3
    with sess:
        out = p()

    assert out == {"out_1": 1, "out_2": 2, "out_3": 3}


def test_linear_pipeline_shared_args(sess):
    """
    Test the execution of a linear pipeline with a shared arguments
    """

    @Craft()
    def step_1(VALUE) -> Volatile("out_1"):  # type: ignore
        return VALUE

    @Craft()
    def step_2(out_1: Volatile, VALUE) -> Volatile("out_2"):  # type: ignore
        return VALUE

    @Craft()
    def step_3(out_2: Volatile, VALUE) -> Volatile("out_3"):  # type: ignore
        return VALUE

    p = step_1 + step_2 + step_3
    with sess:
        out = p(VALUE=3)

    assert out == {"out_1": 3, "out_2": 3, "out_3": 3}


def test_linear_pipeline_namespaced_args(sess):
    """
    Test the execution of a linear pipeline with dispatched arguments
    """

    @Craft()
    def step_1(VALUE) -> Volatile("out_1"):  # type: ignore
        return VALUE

    @Craft()
    def step_2(out_1: Volatile, VALUE) -> Volatile("out_2"):  # type: ignore
        return VALUE

    @Craft()
    def step_3(out_2: Volatile, VALUE) -> Volatile("out_3"):  # type: ignore
        return VALUE

    p = step_1 + step_2 + step_3
    with sess:
        config = {
            "test_pipeline.step_1": {"VALUE": 4},
            "test_pipeline.step_2": {"VALUE": 5},
            "test_pipeline.step_3": {"VALUE": 6},
        }
        out = p(**config)

    assert out == {"out_1": 4, "out_2": 5, "out_3": 6}


def test_linear_pipeline_namespaced_args_and_shared(sess):
    """
    Test the execution of a linear pipeline with both namespaced dispatched arguments and default values
    """

    @Craft()
    def step_1(VALUE, VALUE_2) -> Volatile("out_1"):  # type: ignore
        return VALUE + VALUE_2

    @Craft()
    def step_2(out_1: Volatile, VALUE, VALUE_2) -> Volatile("out_2"):  # type: ignore
        return VALUE + VALUE_2

    @Craft()
    def step_3(out_2: Volatile, VALUE, VALUE_2) -> Volatile("out_3"):  # type: ignore
        return VALUE + VALUE_2

    p = step_1 + step_2 + step_3
    with sess:
        config = {
            "test_pipeline.step_1": {"VALUE": 4},
            "test_pipeline.step_2": {"VALUE": 5},
            "test_pipeline.step_3": {"VALUE": 6},
        }
        out = p(**config, VALUE_2=1)

    assert out == {"out_1": 5, "out_2": 6, "out_3": 7}


def test_linear_pipeline_namespaced_args_and_shared_defaulted(sess):
    """
    Test the execution of a linear pipeline with both namespaced dispatched arguments and defaulted values
    """

    @Craft()
    def step_1(VALUE, VALUE_2=1) -> Volatile("out_1"):  # type: ignore
        return VALUE + VALUE_2

    @Craft()
    def step_2(out_1: Volatile, VALUE, VALUE_2=2) -> Volatile("out_2"):  # type: ignore
        return VALUE + VALUE_2

    @Craft()
    def step_3(out_2: Volatile, VALUE, VALUE_2=1) -> Volatile("out_3"):  # type: ignore
        return VALUE + VALUE_2

    p = step_1 + step_2 + step_3
    with sess:
        config = {
            "test_pipeline.step_1": {"VALUE": 4},
            "test_pipeline.step_2": {"VALUE": 5},
            "test_pipeline.step_3": {"VALUE": 6},
        }
        out = p(**config, VALUE_2=1)

    assert out == {"out_1": 5, "out_2": 6, "out_3": 7}


def test_linear_pipeline_namespaced_args_and_shared_defaulted_overwritted(sess):
    """
    Test the execution of a linear pipeline with both namespaced dispatched arguments and default values overwritted in the shared config
    """

    @Craft()
    def step_1(VALUE, VALUE_2=1) -> Volatile("out_1"):  # type: ignore
        return VALUE + VALUE_2

    @Craft()
    def step_2(out_1: Volatile, VALUE, VALUE_2=2) -> Volatile("out_2"):  # type: ignore
        return VALUE + VALUE_2

    @Craft()
    def step_3(out_2: Volatile, VALUE, VALUE_2=1) -> Volatile("out_3"):  # type: ignore
        return VALUE + VALUE_2

    p = step_1 + step_2 + step_3
    with sess:
        config = {
            "test_pipeline.step_1": {"VALUE": 4},
            "test_pipeline.step_2": {"VALUE": 5},
            "test_pipeline.step_3": {"VALUE": 6},
        }
        out = p(**config, VALUE_2=1)

    assert out == {"out_1": 5, "out_2": 6, "out_3": 7}


# def test_linear_pipeline_namespaced_args_and_shared_defaulted_overwritted
