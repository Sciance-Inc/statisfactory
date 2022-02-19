#! /usr/bin/python3

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
from statisfactory.session import get_active_session

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
        sess_scoped = get_active_session()
        assert sess_scoped is sess
