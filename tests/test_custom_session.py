#! /usr/bin/python3

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

import os
import sys
import pytest
from pathlib import Path

#############################################################################
#                                  Scripts                                  #
#############################################################################


@pytest.fixture
def change_test_dir(request):
    """
    Change the working directory of the test
    """

    p = Path(request.fspath.dirname) / request.param
    # Temporaly change the working directory
    os.chdir(p)
    # Add p to the path tto be explored, to fake lunching a program fro, the cwd
    sys.path.insert(0, str(p))
    yield
    os.chdir(request.config.invocation_dir)
    sys.path.pop(0)


@pytest.mark.parametrize("change_test_dir", ["test_custom_session/"], indirect=True)
def test_session_instanciation(change_test_dir):
    """
    Make sure the session can be instanciated
    """

    from pathlib import Path
    from statisfactory import Session

    factory = Session.get_factory()
    sess = factory()

    assert sess.custom_session_flag == 1
