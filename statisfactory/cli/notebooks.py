#! /usr/bin/python3

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

# system
import os
from pathlib import Path
import re
from typing import List
from itertools import groupby, accumulate

# project
from .logger import get_module_logger

# third party
from nbconvert import PythonExporter
from nbconvert.preprocessors import Preprocessor
from traitlets.config import Config


#############################################################################
#                                  Script                                   #
#############################################################################

# constant
LOGGER = get_module_logger(__name__)


# Constants
_pattern_number_prefix = re.compile(r"^[0-9]+_", re.IGNORECASE)

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
        return all functions definitions (name only) from a sting
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


def build_notebooks(src: Path, dst: Path):
    """
    Recursively parse the Notebooks from 'src', to extract the Craft's definition.
    Definitions found that way are copied to their .py counterpart.
    The name of the py file is the name of the notebook with num purged.

    The functions definitions found in "export" tagged cells, are exported to the root of the package.
    """

    c = Config()
    c.PythonExporter.preprocessors = [_CraftExtractor]
    EXPORTER = PythonExporter(config=c)

    LOGGER.info("bild : building the crafts....")

    # Recursively iterate over the notebooks defined in analytics
    export = []
    for file in src.glob("**/*.ipynb"):

        if "checkpoint" in str(file):
            continue

        # Getting the .py version
        LOGGER.debug(f"build : exporting '{file}'")
        src, meta = EXPORTER.from_filename(file)

        # Expurgate the destination from numer templates, not supported by the thonpy import mechanisme.
        parts = [_pattern_number_prefix.sub("", item) for item in file.parts]

        # Write to the destination
        dst = dst.joinpath(*parts[2:-1], parts[-1].split(".")[0] + ".py")

        dst.parent.resolve().mkdir(parents=True, exist_ok=True)
        with open(dst, "w", encoding="UTF-8") as f:
            f.writelines(src)

        # Append the name of the functions to export
        parts = [item.split(".py")[0] for item in dst.parts[2:]]
        parents = list(accumulate(dst.parts, lambda x, y: x + os.path.sep + y))[1:]

        # Expurgate all path and name of numerica pattern, no supported in the init
        parts = [_pattern_number_prefix.sub("", item) for item in parts]
        parents = [_pattern_number_prefix.sub("", item) for item in parents]

        for exported in meta["exported"]:
            for path, target in zip(parents, parts):
                export.append((path, target, exported))

    # Write the init
    LOGGER.info("build : populating the '__init__'")
    export = sorted(export, key=lambda x: x[0])
    for path, group in groupby(export, key=lambda x: x[0]):
        src = "\n".join((f"from .{item[1]} import {item[2]}  # noqa" for item in group))
        LOGGER.debug(f"build : writting '__init__.py' : {path}")
        with open(Path(path).resolve() / "__init__.py", "w", encoding="UTF-8") as f:
            f.writelines(src)


if __name__ == "__main__":
    raise RuntimeError("can't be run in standalone")
