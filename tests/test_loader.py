#! /usr/bin/python3
#
#    Statisfactory - A satisfying statistical factory
#    Copyright (C) 2021-2022  Hugo Juhel ``
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
# test_loader.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Test the Pipeline / parameters loader
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

from pathlib import Path
import pytest
from statisfactory import Session
from statisfactory.models import PipelineDefinition, ParametersSetDefinition, MergeMethod
from statisfactory.loader.parameters_loader import _merge_by_precedence, get_parameters
from statisfactory.loader.pipelines_loader import get_pipelines
from statisfactory.loader.yaml_utils import gen_as_model

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


def test_deserialization_pipelines():
    """
    Test if a pipeline representation can be deserialized
    """

    target = {"+operators": ["first_operator", "second_operator"]}
    pipeline = PipelineDefinition(**target)

    assert pipeline.operators == target["+operators"]


def test_deserialization_parameterset():
    """
    Test if a ParametersSetDefinition representation can be deserialized
    """

    target = {
        "+from": ["first_set", "second_set"],
        "+merge": "override",
        "+priority": 1,
    }
    params = ParametersSetDefinition(**target)

    assert params.from_ == target["+from"]
    assert params.merge == MergeMethod.override


def test_parameters_precedence():
    """
    Test if the precedence of the parameters set is correctly handled
    """

    p = Path("tests/test_loader/parameters/override/").absolute()

    models = gen_as_model(path=p, model=ParametersSetDefinition)  # type: ignore
    out = _merge_by_precedence(models)  # type: ignore

    assert out["cssvdc_parameters"].b == 2


def test_get_parameters_inheritance_and_precedence(sess):
    """
    Test if the get_parameters works a as whole
    """

    p = Path("tests/test_loader/parameters/override/").absolute()
    params = get_parameters(path=p, session=sess)

    assert params["cssvdc_parameters"]["b"] == 2
    assert params["cssvdc_parameters"]["a"] == 1


def test_get_pipelines(sess):
    """
    Test if pipelines are correctly parsed
    """

    p = Path("tests/test_repo/Pipelines/definitions").absolute()
    pipelines = get_pipelines(path=p, session=sess)

    assert [craft.name for craft in pipelines["multiples_files_pipeline"].crafts] == ["craft_spam"]
    assert [craft.name for craft in pipelines["full"].crafts] == ["craft_foo", "craft_spam"]


def test_merging_strategies(sess):
    """
    Test the recursive and the overriding merge of parameters
    """

    p = Path("tests/test_loader/parameters/merge_data.yaml").absolute()
    params = get_parameters(path=p, session=sess)

    assert params["inherited_2"] == {"param_1": 1, "param_2": 2, "param_nested": {"param_nested_2": 2}, "tags": []}
    assert params["inherited_3"] == {"param_1": 1, "param_2": 2, "param_nested": {"param_nested_1": 1, "param_nested_2": 2}, "tags": []}


def test_support_for_nul(sess):
    """
    Test that Globals and Locals Null are correctly supported
    """

    assert sess.settings.value.not_null == 1
    assert sess.settings.value.nullable is None

    p = Path("tests/test_loader/parameters/nullable.yaml").absolute()
    parameters = get_parameters(path=p, session=sess)

    assert parameters["test"]["nullable"] is None
