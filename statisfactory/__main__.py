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

# project
from cli import build_notebooks
from logger import get_module_logger

# third party
import click
from dynaconf import Dynaconf, Validator

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

    root_dir = Path(path).parent

    # Parse the root file to extract the config
    settings = Dynaconf(
        validators=[
            Validator("sources", default="Lib"),
            Validator("notebook_target", default="jupyter"),
            Validator("notebook_sources", default="Script"),
        ],
        settings_files=[path],
    )

    # Create a settings mapping to cascade to the other commands
    m = {}
    # Add the root path to the targets and sources dir
    m["notebook_target"] = (
        root_dir / settings["sources"] / Path(settings["notebook_target"])
    ).resolve()

    m["notebook_sources"] = (root_dir / Path(settings["notebook_sources"])).resolve()

    # Ad the settings to the CLI context so that it's available to any subcommands
    ctx.obj["settings"] = m


@cli.command()
@click.pass_context
def build(ctx):
    ""

    build_notebooks

    import pdb

    pdb.set_trace()
    LOGGER.info("Extracting Crafts...")


if __name__ == "__main__":
    cli(obj={})
