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
# test_smoketest.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Package smoke test. Very high level tests
"""

#############################################################################
#                                 Packages                                  #
#############################################################################


def test_import():
    """
    Check if the library can be imported
    """
    from statisfactory import Session


def test_session_instanciation():
    """
    Make sure the session can be instanciated
    """

    from pathlib import Path

    from statisfactory import Session

    p = str(Path("tests/test_repo/").absolute())
    sess = Session(root_folder=p)


def test_config_yml():
    from pathlib import Path

    from statisfactory import Session

    p = str(Path("tests/test_repo_yml/").absolute())
    sess = Session(root_folder=p)
