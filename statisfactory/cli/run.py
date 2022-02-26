#! /usr/bin/python3

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
