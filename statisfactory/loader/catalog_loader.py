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
from typing import Union, Dict
from pathlib import Path
from itertools import chain

# project
from statisfactory.loader.yaml_utils import gen_dictionaries_representation

from statisfactory.models.models import Artifact

#############################################################################
#                                  Script                                   #
#############################################################################


def get_artifacts_mapping(path: Union[str, Path], session) -> Dict[str, Artifact]:
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
    catalog_data = {}
    for artifact_data in chain.from_iterable(mappers):
        artifact = Artifact(**artifact_data)  # type: ignore # Validate the struct
        catalog_data[artifact.name] = artifact  # type: ignore

    # TODO: add support for keys collision
    return catalog_data  # type: ignore
