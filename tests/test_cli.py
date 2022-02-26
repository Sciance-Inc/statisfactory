#! /usr/bin/python3

# test_catalog.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Test the catalogue
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

from pathlib import Path
from click.testing import CliRunner
from statisfactory.cli import cli


#############################################################################
#                                 Packages                                  #
#############################################################################


def test_run():
    """
    Check that the cli is actually running and registering the custom hooks.
    """

    # Prepare the command to be executed
    path = Path("tests/test_cli").resolve().absolute()
    args = ["-p", str(path), "run", "base"]

    runner = CliRunner()
    result = runner.invoke(cli, args)

    assert result.exit_code == 0
    assert 'REGISTERING CUSTOM SESSION HOOK' in result.stdout

    print(result)



def test_run():
    """
    Check that the cli is actually running and registering the custom hooks.
    """

    # Prepare the command to be executed
    path = Path("tests/test_cli").resolve().absolute()
    args = ["-p", str(path), "run", "base"]

    runner = CliRunner()
    result = runner.invoke(cli, args)

    assert result.exit_code == 0
    assert 'REGISTERING CUSTOM SESSION HOOK' in result.stdout

    print(result)
