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
# test_active_session.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Check that scoped session are....actually scoped
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

import pytest
from pathlib import Path
from statisfactory import Session

#############################################################################
#                                  Scripts                                  #
#############################################################################


@pytest.fixture
def sess():
    """
    Create a Stati session
    """

    p = str(Path("tests/test_repo/").absolute())
    sess = Session(root_folder=p)

    return sess


def test_active_session(sess):
    """
    Check if calling a session from a with statement actually scopes the session.
    """

    with sess:
        sess_scoped = Session.get_active_session()
        assert sess_scoped is sess
