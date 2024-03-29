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
# dictionaryMerger.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements a way to merge to merge two dictionaries while checking for key collision
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# System
from typing import Dict
from warnings import warn

# Project
from statisfactory.errors import Warnings

#############################################################################
#                                 Packages                                  #
#############################################################################


def merge_dictionaries(left: Dict, right: Dict, strict=True) -> Dict:
    """
    Return a new dictionary by merging Left and Right dictionaries together.
    Raise a KeyError if strict is True and keys collides.
    """

    # Prevents dealing with one dict or two being None
    left = left or {}
    right = right or {}

    colliding_keys = set(left.keys()).intersection(set(right.keys()))
    is_collision = len(colliding_keys) > 0

    if is_collision and strict:
        raise KeyError(f"Colliding keys : {', '.join(colliding_keys)}")

    if is_collision:
        warn(Warnings.W050.format(keys=", ".join(colliding_keys)))  # type: ignore

    return {**left, **right}


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("can't be run in standalone")
