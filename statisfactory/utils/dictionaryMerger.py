#! /usr/bin/python3

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

# Project
from ..errors import warnings

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
        warnings.W055(__name__, keys=", ".join(colliding_keys))

    return {**left, **right}


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("can't be run in standalone")
