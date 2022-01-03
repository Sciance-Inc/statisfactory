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

# project
from statisfactory.cli import build_notebooks
from dynaconf import Dynaconf, Validator
from logger import get_module_logger

#############################################################################
#                                  Script                                   #
#############################################################################

# constant
LOGGER = get_module_logger(__name__)


@click.group()
@click.option(
    "-p",
    "--path",
    default="statisfactory.yaml",
    type=click.Path(exists=True),
    help="An optional path to the 'statisfactory.yaml' settings file",
)
@click.pass_context
def cli(ctx, path):
    ctx.ensure_object(dict)
    path = Path(path)

    if not path.is_file():
        raise FileNotFoundError("Path must be a file, got dir")

    root_dir = path.parent

    # Parse the root file to extract the config
    settings = Dynaconf(
        validators=[
            Validator("sources", default="Lib"),
            Validator("notebook_target", default="jupyter"),
            Validator("notebook_sources", default="Script"),
        ],
        settings_files=[path],
    )

    # Create a settings mapping to be cascaded to the other commands
    m = {}
    m["root_dir"] = root_dir
    m["sources"] = settings["sources"]
    m["notebook_target"] = settings["notebook_target"]
    m["notebook_sources"] = settings["notebook_sources"]

    # Ad the settings to the CLI context so that it's available to any subcommands
    ctx.obj["settings"] = m


@cli.command()
@click.pass_context
def build(ctx):
    """
    Parse the notebooks folders and build the Crafts definitions.
    Extract the Craft definitions to the notebook targets folder.
    """
    LOGGER.info("Building the Crafts...")
    m = ctx.obj["settings"]

    # Build the Notebooks sources and targets directorues
    notebook_target = m["root_dir"] / m["sources"] / Path(m["notebook_target"])
    notebook_target = notebook_target.resolve()

    notebook_sources = m["root_dir"] / Path(m["notebook_sources"])
    notebook_sources = notebook_sources.resolve()

    build_notebooks(notebook_sources, notebook_target)

    return


if __name__ == "__main__":
    cli(obj={})
