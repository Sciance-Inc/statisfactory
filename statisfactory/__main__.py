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

# project
from logger import get_module_logger

# third party
import click


#############################################################################
#                                  Script                                   #
#############################################################################

# constant
LOGGER = get_module_logger(__name__)


@click.group()
@click.argument("path")
def cli(path):
    pass


@cli.command()
def init():
    pass


@cli.command()
@click.option("--name", help="the name of the pipeline to run.")
@click.argument("path")
def run(name: str, path: str):
    """Run the pipeline

    Args:
        name (str): the name of the pipeline to run
    """

    pass


if __name__ == "__main__":
    LOGGER.info("starting statisfactory")
    cli(path="1")
