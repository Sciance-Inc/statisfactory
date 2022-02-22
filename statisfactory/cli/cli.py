#! /usr/bin/python3

# cli.py
#
# Project name: statisfactory.
# Author: Hugo Juhel
#
# description:
"""
    implements the statisfactory's cli
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from pathlib import Path

# third party
import click

from statisfactory.cli import build_notebooks, run_pipeline, temp_wd
from statisfactory.logger import get_module_logger
from statisfactory.loader import get_pyproject, get_path_to_target

#############################################################################
#                                  Script                                   #
#############################################################################

# constant
LOGGER = get_module_logger("statisfactory")


@click.group()
@click.option(
    "-p",
    "--path",
    default=None,
    type=click.Path(exists=True),
    help="An optional path to the repository to run the commandas from.",
)
@click.pass_context
def cli(ctx, path):
    ctx.ensure_object(dict)

    if path:
        path = Path(path).absolute() / "pyproject.toml"
    else:
        path = get_path_to_target("pyproject.toml")

    if not path.is_file():
        raise FileNotFoundError("Path must points to a 'pyproject.toml' file.")

    # Ad the settings to the CLI context so that it's available to any subcommands
    ctx.obj["path"] = path
    ctx.obj["root"] = path.parent


@cli.command()
@click.pass_context
def compile(ctx):
    """
    Parse the notebooks folders and build the Crafts definitions.
    Extract the Craft definitions to the notebook targets folder.
    """
    LOGGER.info("Building the Crafts...")

    # Extract the path to parse from and to
    path = ctx.obj["path"]
    root_dir = ctx.obj["root"]

    # Extract values from pyproject
    pyproject = get_pyproject(path)

    # Build paths to be forwarded to the parser
    target = root_dir / pyproject.sources / pyproject.notebook_target
    source = root_dir / pyproject.notebook_sources

    # Solve paths
    target = target.resolve()
    source = source.resolve()

    build_notebooks(source, target)

    return


@cli.command()
@click.pass_context
@click.argument("pipeline")
@click.option(
    "-p",
    "--parameters",
    default=None,
    type=str,
    help="A configuration to be used for this pipeline run.",
)
def run(ctx, pipeline: str, parameters: str):
    """
    Run a pipeline with a given configuraiton.

    Args:
        pipeline (str): The pipeline to be executed.
        parameters (str): An optional name for a set of parameters defined in the parameters object of statisfactory.
    """

    with temp_wd(ctx.obj["root"]):
        run_pipeline(
            path=ctx.obj["root"], pipeline_name=pipeline, parameters_name=parameters
        )
