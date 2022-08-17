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
# test_custom_session.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Check if a custom session can be used in place of the stati default one.
"""

#############################################################################
#                                 Packages                                  #
#############################################################################


import pytest
from statisfactory.IO import MixinInterpolable

#############################################################################
#                                  Scripts                                  #
#############################################################################


class Interpolator(MixinInterpolable):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


interpolator = Interpolator()


def test_dynamic_interpolation_simple():
    """
    Test the simple interpolation of a string
    """

    # With a string
    interpolated = interpolator._interpolate("Hello +{name}", name="World")
    assert interpolated == "Hello World"

    # With an integer
    interpolated = interpolator._interpolate("Hello +{name}", name=1234)
    assert interpolated == "Hello 1234"


def test_dynamic_interpolation_multiple():
    """
    Test the multiple interpolation of a string
    """
    interpolated = interpolator._interpolate("Hello +{name} +{value}", name="World", value=1234)
    assert interpolated == "Hello World 1234"


def test_dynamic_interpolation_alternatives_types():
    """
    Test the multiple interpolation of either an integer or a None
    """
    interpolated = interpolator._interpolate("+{value}", value=None)
    assert interpolated == "None"
