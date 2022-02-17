#! /usr/bin/python3

# catalog_loader.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Load the Catalogs files
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from typing import Union
from pathlib import Path

# project
from statisfactory.loader.yaml_utils import (
    gen_dictionaries_representation,
    merge_dict,
)

from statisfactory.models.models import CatalogData

#############################################################################
#                                  Script                                   #
#############################################################################


def get_catalog_data(path: Union[str, Path], session) -> CatalogData:
    """
    build the catalog data

    Returns:
        CatalogData: A mapping of artifacts and connectors
    """

    render_vars = {k.lower(): v for k, v in session.settings.to_dict().items()}
    path = Path(path)

    # merge all the yamls, concatenate the various artifacts and connectors
    mappers = gen_dictionaries_representation(path, render_vars)

    # Deserialize each mapper, and validate it against the model
    catalogs = []
    for mapper in mappers:
        catalogs.append(CatalogData(**mapper))

    # TODO: add support for keys collision
    artifacts = merge_dict(*(c.artifacts for c in catalogs))
    connectors = merge_dict(*(c.connectors for c in catalogs))

    return CatalogData(artifacts=artifacts, connectors=connectors)  # type: ignore
