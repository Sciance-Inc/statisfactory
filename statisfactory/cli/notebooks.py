#! /usr/bin/python3
#
#    Statisfactory - A satisfying statistical factory
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
# notebooks.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements the notebooks parser for the Statisfactory's CLI
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

import re
from itertools import accumulate

# system
from pathlib import Path
from typing import List

# third party
from nbconvert import PythonExporter
from nbconvert.preprocessors import Preprocessor
from traitlets.config import Config

from statisfactory.logger import get_module_logger  # .logger

#############################################################################
#                                  Script                                   #
#############################################################################

# constant
LOGGER = get_module_logger("statisfactory")


# Constants
_pattern_number_prefix = re.compile(r"^[0-9]+_", re.IGNORECASE)
_pattern_spaces = re.compile(r"\s+")

#############################################################################
#                                  Classes                                  #
#############################################################################


class _CraftExtractor(Preprocessor):
    """
    Extract the "craft" tagged cells from a notebook
    """

    _func_pattern = re.compile(r"\ndef (\w+)\s*\((.*?)\):")

    def _get_all_functions(self, src) -> List[str]:
        """
        Get all functions definitions (name only) from a sting
        """

        names = []
        for name, _ in self._func_pattern.findall(src):
            names.append(name)

        return names

    def preprocess(self, nb, ressources):
        out = []
        exported = []
        for cell in nb.cells:

            tags = [item.lower() for item in cell.metadata.get("tags", [])]
            if "craft" in tags:
                out.append(cell)
            if "export" in tags:
                exported.extend(self._get_all_functions(cell.source))

        nb.cells = out
        ressources["exported"] = exported
        return nb, ressources


def _target_path(path: Path) -> Path:
    """
    Clean-up a path by removing all numeric from it
    """

    # Remove the number from , not supported by the thonpy import mechanisme.
    parts = (_pattern_number_prefix.sub("", item) for item in path.parts)
    parts = [_pattern_spaces.sub("_", item) for item in parts]

    tmp, file_name = parts[0:-1], parts[-1]
    file_name = file_name.replace("ipynb", "py")

    path = Path(*tmp, file_name)

    return path


def build_notebooks(src: Path, dst: Path):
    """
    Recursively parse the Notebooks from 'src', to extract the Craft's definition.

    Definitions found that way are copied to their .py counterpart.
    The name of the py file is the name of the notebook with num purged.
    The functions definitions found in "export" tagged cells, are exported to the root of the package.
    """

    c = Config()
    c.PythonExporter.preprocessors = [_CraftExtractor]  # type: ignore
    EXPORTER = PythonExporter(config=c)

    LOGGER.info("Exporting the python code.")
    inits = set([])
    # Recursively iterate over the notebooks
    for file in src.glob("**/*.ipynb"):

        if "checkpoint" in str(file):
            continue

        # Build the desition
        target_cursor = dst / _target_path(file.relative_to(src))

        # Parse the notebook
        LOGGER.debug(f"build : exporting '{file}'")
        src_, meta = EXPORTER.from_filename(file)  # type: ignore

        target_cursor.parent.resolve().mkdir(parents=True, exist_ok=True)
        with open(target_cursor, "w", encoding="UTF-8") as f:
            f.writelines(src_)

        # Flag the inits to be created
        parts = (Path(i) for i in target_cursor.parent.relative_to(dst.parent).parts)
        for item in accumulate(parts, lambda x, y: x / y):
            inits.add(dst.parent / item)

    # Create the subpackages inits
    LOGGER.info("Creating the subpackages inits.")
    for path in inits:
        LOGGER.debug(f"build : writting '__init__.py' : {path}")

        with open(Path(path).resolve() / "__init__.py", "w", encoding="UTF-8") as f:
            f.writelines("")


if __name__ == "__main__":
    raise RuntimeError("can't be run in standalone")
