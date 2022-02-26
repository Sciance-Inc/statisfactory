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
from statisfactory import Session
from statisfactory.logger import get_module_logger
from statisfactory.loader import get_pyproject, get_path_to_target
from pydantic.json import pydantic_encoder
import json

#############################################################################
#                                  Script                                   #
#############################################################################

# constant
LOGGER = get_module_logger("statisfactory")


def _prepare_group(ctx, path):
    ctx.ensure_object(dict)

    if path:
        if not path.endswith("pyproject.toml"):
            path = Path(path).absolute() / "pyproject.toml"
    else:
        path = get_path_to_target("pyproject.toml") / "pyproject.toml"

    if not path.is_file():
        raise FileNotFoundError("Path must points to a folder 'pyproject.toml' file.")

    # Ad the settings to the CLI context so that it's available to any subcommands
    ctx.obj["path"] = path
    ctx.obj["root"] = path.parent

    return ctx


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
    ctx = _prepare_group(ctx, path)


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
    "-c",
    "--configuration",
    default=None,
    type=str,
    help="A configuration to be used for this pipeline run.",
)
def run(ctx, pipeline: str, configuration: str):
    """
    Run a pipeline with a given configuraiton.

    Args:
        pipeline (str): The pipeline to be executed.
        parameters (str): An optional name for a set of parameters defined in the parameters object of statisfactory.
    """

    with temp_wd(ctx.obj["root"]):
        run_pipeline(
            path=ctx.obj["root"], pipeline_name=pipeline, parameters_name=configuration
        )


@cli.group()
def pipelines():
    """
    List and describes pipelines
    """
    # ctx = _prepare_group(ctx, path)
    ...


@pipelines.command("ls")
@click.pass_context
def pip_ls(ctx):
    """
    List the pipelines
    """

    sess = Session(root_folder=ctx.obj["root"])

    n_pipelines = len(sess.pipelines_definitions)
    string_pipelines = "\n - ".join(sess.pipelines_definitions)

    string = "\n - ".join((f"Found {n_pipelines} pipelines :", string_pipelines))

    print(string)


@pipelines.command("describe")
@click.pass_context
@click.argument("name")
def pip_describe(ctx, name: str):
    """
    Describe the dependencies and execution flow of a pipeline named 'name'

    Args:
        name (str): the name of the pipeline to describe
    """

    sess = Session(root_folder=ctx.obj["root"])
    pipeline = sess.pipelines_definitions[name]

    string_operators = "\n - ".join(
        (
            f"Describing the pipeline '{pipeline.name}' :",
            "\n - ".join(c.name for c in pipeline.crafts),
        )
    )
    print(string_operators)
    print(pipeline)


@cli.group()
def configurations():
    """
    List and describe
    """
    # ctx = _prepare_group(ctx, path)
    ...


@configurations.command("ls")
@click.pass_context
def conf_ls(ctx):
    """
    List the configurations
    """

    sess = Session(root_folder=ctx.obj["root"])

    n_confs = len(sess.parameters)
    string_configurations = "\n - ".join(sess.parameters)

    string = "\n - ".join((f"Found {n_confs} configurations :", string_configurations))
    print(string)


@configurations.command("describe")
@click.pass_context
@click.argument("name")
def conf_describe(ctx, name: str):
    """
    Describe the configurations

    Args:
        name (str): the name of the configuration to describe
    """

    sess = Session(root_folder=ctx.obj["root"])
    configuration = sess.parameters[name]

    print(f"Describing the configuration '{name}' :")
    print(json.dumps(configuration, indent=2))


@cli.group()
def artifacts():
    """
    List and describe the artifacts
    """
    # ctx = _prepare_group(ctx, path)
    ...


@artifacts.command("ls")
@click.pass_context
def artifacts_ls(ctx):
    """
    List the artifacts
    """

    artifacts = Session(root_folder=ctx.obj["root"]).catalog.artifacts
    n_artifacts = len(artifacts)
    string_artifacts = "\n - ".join(artifacts)

    string = "\n - ".join((f"Found {n_artifacts} artifacts :", string_artifacts))
    print(string)


@artifacts.command("describe")
@click.pass_context
@click.argument("name")
def artifacts_describe(ctx, name: str):
    """
    Describe the artifact

    Args:
        name (str): the name of the artifact to describe
    """

    artifacts = Session(root_folder=ctx.obj["root"]).catalog.artifacts
    artifact = artifacts[name]

    print(f"Describing the Artifact '{name}' :")
    print(json.dumps(artifact, default=pydantic_encoder, indent=2))
