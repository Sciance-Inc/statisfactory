#! /usr/bin/python3

# main.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    implements various backend to be reused across artefacts
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from __future__ import annotations  # noqa

from abc import ABCMeta, abstractmethod
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import ParseResult
from warnings import warn

from ...errors import Errors, Warnings
from ...logger import MixinLogable

# Project type checks : see PEP563
if TYPE_CHECKING:
    from ...session import Session

#############################################################################
#                                  Script                                   #
#############################################################################


class Backend(MixinLogable, metaclass=ABCMeta):
    """
    Interface for the low level communication with a service (s3, lakefs...) through the exchange of Bytes
    The backend consume and returns Bytes.
    """

    def __init__(self, session: Session, logger_name: str = __name__):
        """
        Instanciate a new S3Backend. Implemeted for cooperative multiple inheritance only.

        Args:
            session (Session): The Statisfactory session to use.
        """

        super().__init__(logger_name=logger_name)
        self._session = session

    @abstractmethod
    def put(self, *, payload: bytes, fragment: ParseResult):
        """
        Drop the payload to the service under the 'path' name.

        Args:
            payload (bytes): The bytes representation of the artifact to drop on the backend.
            fragment (ParseResult): The Artefact's path parsed result to use to put the payload to.
        """
        ...

    @abstractmethod
    def get(self, *, fragment: ParseResult) -> bytes:
        """
        Get the payload from the service under the 'path' name.

        Args:
            fragment (ParseResult): The Artefact's path parsed result to use to fetch the payload from.
        """
        ...


class S3Backend(Backend):
    """
    Write and fetch data from S3
    """

    def __init__(self, session: Session):
        """
        Instanciate a new S3Backend
        """

        super().__init__(session=session)  # type: ignore

        # Build a new s3 ressource from the AWS_SESSION
        aws_s3_endpoint = self._session.settings.get("AWS_S3_ENDPOINT", None)  # type: ignore
        if aws_s3_endpoint:
            self._s3 = self._session.aws_session.resource(
                "s3", endpoint_url=aws_s3_endpoint
            )
        else:
            self._s3 = self._session.aws_session.resource("s3")
            warn(Warnings.W021())  # type: ignore

    def put(self, *, payload: bytes, fragment: ParseResult):
        """
        Drop the payload to S3 using the data from the Fragment

        Args:
            payload (bytes): The bytes representation of the artifact to drop on the backend.
            fragment (ParseResult): The Artefact's path parsed result.
        """

        bucket = fragment.netloc
        dst = fragment.path

        try:
            resp = self._s3.Object(bucket, dst).put(Body=BytesIO(payload))
            if not resp["ResponseMetadata"]["HTTPStatusCode"] == 200:
                raise ValueError("S3 returned a non 200 status code")
        except BaseException as error:
            raise Errors.E0290(backend="S3Backend") from error  # type: ignore

    def get(self, *, fragment: ParseResult) -> bytes:
        """
        Get the payload from the service under the 'path' name.

        Args:
            fragment (ParseResult): The Artefact's path parsed result to use to fetch the payload from.
        """

        bucket = fragment.netloc
        src = fragment.path

        try:
            resp = self._s3.Object(bucket, src).get()
            if not resp["ResponseMetadata"]["HTTPStatusCode"] == 200:
                raise ValueError("S3 returned a non 200 status code")
        except BaseException as error:
            raise Errors.E0291(backend="S3Backend") from error  # type: ignore

        return resp["Body"].read()


class LocalFS(Backend):
    """
    Write and fetch data from the local file system
    """

    def __init__(self, session: Session):
        """
        Instanciate a new Local File System
        """

        super().__init__(session=session)  # type: ignore

    def _create_parents(self, path: Path):
        """
        Create all the parents of a given path.

        Args:
            path (Path): the path to create the parent from
        """

        path.parents[0].mkdir(parents=True, exist_ok=True)

    def put(self, *, payload: bytes, fragment: ParseResult):
        """
        Drop the payload to the file system using the data from the Fragment

        Args:
            payload (bytes): The bytes representation of the artifact to drop on the backend.
            fragment (ParseResult): The Artefact's path parsed result.
        """

        path = Path(fragment.path).absolute()
        self._create_parents(path)

        with open(path, "wb+") as f:
            f.write(payload)

    def get(self, *, fragment: ParseResult) -> bytes:
        """
        Get the payload from the service under the 'path' name.

        Args:
            fragment (ParseResult): The Artefact's path parsed result to use to fetch the payload from.
        """

        path = Path(fragment.path).absolute()

        try:
            with open(path, "rb") as f:
                payload = f.read()
        except FileNotFoundError as err:
            raise Errors.E024(path=path) from err  # type: ignore

        return payload


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("interface.py can't be run in standalone")
