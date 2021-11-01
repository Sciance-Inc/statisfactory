#! /usr/bin/python3

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
