#! /usr/bin/python3

# main.py
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

from statisfactory.cli import build_notebooks
from statisfactory.logger import get_module_logger
from statisfactory.loader import get_pyproject, get_path_to_target

#############################################################################
#                                  Script                                   #
#############################################################################

# constant
LOGGER = get_module_logger(__name__)


@click.group()
@click.option(
    "-p",
    "--path",
    default=None,
    type=click.Path(exists=True),
    help="An optional path to the 'pyproject.toml' file.",
)
@click.pass_context
def cli(ctx, path):
    ctx.ensure_object(dict)

    if path:
        path = Path(path).absolute()
    else:
        path = get_path_to_target("pyproject.toml")

    if not path.is_file():
        raise FileNotFoundError("Path must points to a 'pyproject.toml' file.")

    # Ad the settings to the CLI context so that it's available to any subcommands
    ctx.obj["path"] = path


@cli.command()
@click.pass_context
def build(ctx):
    """
    Parse the notebooks folders and build the Crafts definitions.
    Extract the Craft definitions to the notebook targets folder.
    """
    LOGGER.info("Building the Crafts...")

    # Extract the path to parse from and to
    path = ctx.obj["path"]
    root_dir = path.parent

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


if __name__ == "__main__":
    cli(obj={})
