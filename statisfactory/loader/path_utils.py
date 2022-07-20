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
