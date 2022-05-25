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
# run.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements primitives to run the pipelines
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

from pathlib import Path
from statisfactory import Session
from statisfactory.errors import Errors

#############################################################################
#                                   Script                                  #
#############################################################################


def run_pipeline(path: Path, pipeline_name: str, parameters_name: str = None):
    """
    Execute a pipeline against a provided configuration

    Args:
        path (Path): the path to the statisfactory-enable repo to load.
        pipeline_name (str): the name of the pipeline to execute.
        config_name (str, optional): An optional name of arguments set to be fetched from the sessioon parameters. Defaults to None.
    """

    sess = Session(root_folder=path)

    # Fetch the pipeline and the parameters set to be executed
    try:
        pip = sess.pipelines_definitions[pipeline_name]
    except KeyError:
        raise Errors.E071(pipeline_name=pipeline_name) from None  # type: ignore

    cfg = {}
    if parameters_name:
        try:
            cfg = sess.parameters[parameters_name]
        except KeyError:
            raise Errors.E070(parameters_name=parameters_name) from None  # type: ignore

    # Execute the pipeline
    with sess:
        pip(**cfg)


if __name__ == "__main__":
    raise RuntimeError("can't be run in standalone")
