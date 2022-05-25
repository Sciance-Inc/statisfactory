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


def test_variadic_artifact_interpolation_dispatching(sess):
    """
    Check if the variadic arguments are correctly dispatched for interpolations
    """

    @Craft()
    def spam(test_read_variadic_pickle: Artifact) -> Volatile("step_1"):  # type: ignore
        return test_read_variadic_pickle

    @Craft()
    def foo(step_1: Volatile) -> Volatile("step_2"):  # type: ignore
        return step_1

    p = spam + foo
    with sess:
        p(variadic="data")


def test_multiples_files_pipelines(sess):
    """
    Test if pipelines can be parsed from multiples files
    """

    _ = sess.pipelines_definitions["multiples_files_pipeline"]


def test_volatile_default(sess):
    """
    Make sure than a pipeline can be called with defaults volatiles
    """

    @Craft()
    def bar(bar: Volatile = 1) -> Volatile("bar_out"):
        return bar

    @Craft()
    def spam(bar_out: Volatile) -> Volatile("spam_out"):
        return bar_out

    foo = spam + bar

    with sess:
        out = foo()

    assert out["spam_out"] == 1
