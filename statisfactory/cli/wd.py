#! /usr/bin/python3

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
