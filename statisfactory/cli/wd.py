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
# wd.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    helpers to temporaly alter the working directory stati is executed in
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

from contextlib import contextmanager
from pathlib import Path
import os
import sys

#############################################################################
#                                   Script                                  #
#############################################################################


@contextmanager
def temp_wd(path: Path):
    """
    Context manager. Temporary set the working dir to 'path'.

    Args:
        path (Path): the path to the temp working dir to set.
    """

    cwd = os.getcwd()
    os.chdir(path)
    sys.path.insert(0, str(path))
    yield
    os.chdir(cwd)
    sys.path.pop(0)
    return
