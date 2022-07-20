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
# manifest.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements the manifest parser for the Statisfactory's CLI
"""

# TODO: The Manifest's parser as to be moved to DepecheCode, but I still have some kinks to iron out. For now, the Manifest dataclass is duplicated in depecheCode (aie)

#############################################################################
#                                 Packages                                  #
#############################################################################


from pathlib import Path
from statisfactory import Session
from statisfactory.models import Manifest
from statisfactory.operator.pipeline.solver import DAGSolver
from statisfactory.operator.annotations import AnnotationKind


def _extract_annotation(annotations):
    out = {"volatiles": [], "artifacts": []}
    for annotation in annotations:
        if annotation.kind == AnnotationKind.VOLATILE:
            out["volatiles"].append(annotation.name)
        elif annotation.kind == AnnotationKind.ARTEFACT:
            out["artifacts"].append(annotation.name)

    return out


def build_manifest(sess: Session, path: Path):
    """
    Build the manifest to statically specify dependencies between pipelines.
    """

    pipelines = {}
    for pipeline_name, pipeline in sess.pipelines_definitions.items():  # type: ignore
        steps = []
        for batch in DAGSolver(pipeline.crafts):
            batch = [
                {
                    "module": craft.__module__,
                    "name": craft.__name__,
                    "inputs": _extract_annotation(craft.input_annotations),
                    "outputs": _extract_annotation(craft.output_annotations),
                }
                for craft in batch
            ]
            steps.append(batch)
        pipelines[pipeline_name] = steps

    manifest = Manifest(pipelines=pipelines)

    with open(path, "w") as f:
        manifest.json_dump(f)
