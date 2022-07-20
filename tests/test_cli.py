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
    assert "REGISTERING CUSTOM SESSION HOOK" in result.stdout

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
    assert "REGISTERING CUSTOM SESSION HOOK" in result.stdout

    print(result)
