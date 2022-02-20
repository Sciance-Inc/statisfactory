#! /usr/bin/python3

# pyproject_loader.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements utility functions to fetch the pyproject.toml
"""

#############################################################################
#                                 Packages                                  #
#############################################################################


from pathlib import Path

#############################################################################
#                                  Script                                   #
#############################################################################


def get_path_to_target(target: str) -> Path:
    """
    Retrieve the path to "target" file by executing a "fish pass ;)" from the location of the caller
    """

    # Retrieve the "statisfactory.yaml" file
    root = Path("/")
    trg = Path().resolve()
    while True:
        if (trg / target).exists():
            return trg
        trg = trg.parent
        if trg == root:
            raise Errors.E010(target=target)  # type: ignore
