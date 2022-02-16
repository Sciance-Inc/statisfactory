#! /usr/bin/python3

# main.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    implements various backend to be reused across artifacts
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# system
from __future__ import annotations  # noqa

import re
from abc import ABCMeta, abstractmethod
from functools import lru_cache
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Type
from urllib.parse import ParseResult
from warnings import warn

from lakefs_client import models  # type: ignore

from statisfactory.errors import Errors, Warnings
from statisfactory.logger import MixinLogable, get_module_logger

# Project type checks : see PEP563
if TYPE_CHECKING:
    from statisfactory.session import Session

#############################################################################
#                                  Script                                   #
#############################################################################


class Backend(MixinLogable, metaclass=ABCMeta):
    """
    Interface for the low level communication with a service (s3, lakefs...) through the exchange of Bytes
    The backend consume and returns Bytes.
    """

    # A placeholder for all registered backends
    _backends = dict()

    def __init__(self, session: Session, logger_name: str = __name__):
        """
        Instanciate a new S3Backend. Implemeted for cooperative multiple inheritance only.

        Args:
            session (Session): The Statisfactory session to use.
        """

        super().__init__(logger_name=logger_name)
        self._session = session

    def __init_subclass__(cls, prefix: str, **kwargs):
        """
        Implement the registration of a child class into the backend class.
        By doing so, the Backend can be extended to use new interactors without updating the code of the class (Open Close principle)
        See PEP-487 for details.

        Args:
            prefix (str): The prefix to register the backend under

        Raises:
            Errors.E0201: Raised if the prefix has already a backend registered under,
        """

        super().__init_subclass__(**kwargs)

        # Register the new interactors into the artifactclass
        if Backend._backends.get(prefix):
            raise Errors.E0201(prefix=prefix)  # type: ignore

        Backend._backends[prefix] = cls

        get_module_logger(__name__).debug(f"Registering '{prefix}' backend")

    @classmethod
    def backends(cls: Type[Backend]) -> Dict[str, Type[Backend]]:
        """
        Getter for the registered backends

        Returns:
            Map[str, Type[Backend]]: A mapping of prefixes associated with their respectives backend.
        """

        return cls._backends

    @abstractmethod
    def put(self, *, payload: bytes, fragment: ParseResult, **kwargs):
        """
        Drop the payload to the service under the 'path' name.

        Args:
            payload (bytes): The bytes representation of the artifact to drop on the backend.
            fragment (ParseResult): The Artifact's path parsed result to use to put the payload to.
        """
        ...

    @abstractmethod
    def get(self, *, fragment: ParseResult, **kwargs) -> bytes:
        """
        Get the payload from the service under the 'path' name.

        Args:
            fragment (ParseResult): The Artifact's path parsed result to use to fetch the payload from.
        """
        ...


class S3Backend(Backend, prefix="s3"):
    """
    Write and fetch data from S3
    """

    def __init__(self, session: Session):
        """
        Instanciate a new S3Backend
        """

        super().__init__(session=session)  # type: ignore

        # Build a new s3 ressource from the AWS_SESSION
        aws_s3_endpoint = self._session.settings.get("aws_s3_endpoint", None)  # type: ignore
        if aws_s3_endpoint:
            self._s3 = self._session.aws_session.resource(
                "s3", endpoint_url=aws_s3_endpoint
            )
        else:
            self._s3 = self._session.aws_session.resource("s3")
            warn(Warnings.W021())  # type: ignore

    @lru_cache()
    def _create_bucket(self, bucket: str):
        """
        Create the S3 Bucket if required

        Args:
            bucket (str): The bucket's name to create.
        """

        bucket_exists = self._s3.Bucket(bucket) in self._s3.buckets.all()
        if bucket_exists:
            return

        self._s3.create_bucket(Bucket=bucket)

    def put(self, *, payload: bytes, fragment: ParseResult, **kwargs):
        """
        Drop the payload to S3 using the data from the Fragment

        Args:
            payload (bytes): The bytes representation of the artifact to drop on the backend.
            fragment (ParseResult): The Artifact's path parsed result.
        """

        bucket = fragment.netloc
        dst = fragment.path

        # Create the bucket
        self._create_bucket(bucket)

        try:
            resp = self._s3.Object(bucket, dst).put(Body=BytesIO(payload))
            if not resp["ResponseMetadata"]["HTTPStatusCode"] == 200:
                raise ValueError("S3 returned a non 200 status code")
        except BaseException as error:
            raise Errors.E0290(backend="S3Backend") from error  # type: ignore

    def get(self, *, fragment: ParseResult, **kwargs) -> bytes:
        """
        Get the payload from the service under the 'path' name.

        Args:
            fragment (ParseResult): The Artifact's path parsed result to use to fetch the payload from.
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


class LocalFS(Backend, prefix=""):
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

    def put(self, *, payload: bytes, fragment: ParseResult, **kwargs):
        """
        Drop the payload to the file system using the data from the Fragment

        Args:
            payload (bytes): The bytes representation of the artifact to drop on the backend.
            fragment (ParseResult): The Artifact's path parsed result.
        """

        path = Path(fragment.path).absolute()
        self._create_parents(path)

        with open(path, "wb+") as f:
            f.write(payload)

    def get(self, *, fragment: ParseResult, **kwargs) -> bytes:
        """
        Get the payload from the service under the 'path' name.

        Args:
            fragment (ParseResult): The Artifact's path parsed result to use to fetch the payload from.
        """

        path = Path(fragment.path).absolute()

        try:
            with open(path, "rb") as f:
                payload = f.read()
        except FileNotFoundError as err:
            raise Errors.E024(path=path) from err  # type: ignore

        return payload


class LakeFSBackend(Backend, prefix="lakefs"):
    """
    Write and fetch data from / to LakeFS
    """

    REG_VALID_SLUG = re.compile(r"^[a-zA-Z0-9\-]*$")

    def __init__(self, session: Session):
        """
        Instanciate a new LakeFS Backend.
        """

        super().__init__(session=session)  # type: ignore

    @lru_cache()
    def _create_branch(self, name: str):
        """
        Create a new Lake FS branch if it does not already exists
        """

        client = self._session.lakefs_client
        repo_name = self._session.lakefs_repo["name"]

        existing_branches = set(
            [item["id"] for item in client.branches.list_branches(repo_name).results]
        )
        if name in existing_branches:
            return

        try:
            client.branches.create_branch(
                repository=repo_name,
                branch_creation=models.BranchCreation(name=name, source="main"),
            )

        except BaseException as error:
            raise Errors.E065() from error  # type: ignore

    def _get_current_branch_name(self) -> str:
        """
        Build a valid LakeFS branch name from the currently checkout in git

        Returns:
            str: The current branch name to use in LakeFS
        """

        # Get the name of the Git branch currently checkout and create it on LakeFS
        branch: str = self._session.git.head.shorthand

        branch = branch.replace("\\", "-")
        branch = branch.replace("/", "-")

        if not re.match(LakeFSBackend.REG_VALID_SLUG, branch):
            raise Errors.E0293(regex=LakeFSBackend.REG_VALID_SLUG)  # type: ignore

        return branch

    def put(
        self, *, payload: bytes, fragment: ParseResult, lake_ref: str = None, **kwargs
    ):
        """
        Drop the payload to the file system using the data from the Fragment

        Args:
            payload (bytes): The bytes representation of the artifact to drop on the backend.
            fragment (ParseResult): The Artifact's path parsed result.
            lake_ref (str, optional): An optional commit / branch / lake ref to put the data to. Defaults to the current gitted branch.
        """

        branch = lake_ref or self._get_current_branch_name()
        self._create_branch(branch)

        # Get the path and maybe remove specific left / as lake object shcould not be prefixed
        path = fragment.path.lstrip("/")

        # Get the name of the repo to write to
        repo = self._session.lakefs_repo["name"]

        # Write the payload to LakeFS
        try:
            self._session.lakefs_client.objects.upload_object(
                repository=repo, branch=branch, path=path, content=BytesIO(payload)
            )
        except BaseException as error:
            raise Errors.E0290(backend="LakeFSBackend") from error  # type: ignore

    def get(self, *, fragment: ParseResult, lake_ref: str = None, **kwargs) -> bytes:
        """
        Get the payload from the service under the 'path' name.

        Args:
            fragment (ParseResult): The Artifact's path parsed result to use to fetch the payload from.
            lake_ref (str, optional): An optional commit / branch / lake ref to fetch the data from. Defaults to the current gitted branch.

        Returns:
            bytes: The payload fetched from lakefs
        """

        # Get the name of the Git branch currently checkout and create it on LakeFS
        branch = lake_ref or self._get_current_branch_name()

        # Get the path and maybe remove specific left / as lake object shcould not be prefixed
        path = fragment.path.lstrip("/")

        # Get the name of the repo to write to
        repo = self._session.lakefs_repo["name"]

        # Write the payload to LakeFS
        try:
            payload = self._session.lakefs_client.objects.get_object(
                repository=repo, ref=branch, path=path
            )
        except BaseException as error:
            raise Errors.E0291(backend="LakeFSBackend") from error  # type: ignore

        return payload.read()


#############################################################################
#                                   main                                    #
#############################################################################
if __name__ == "__main__":
    raise BaseException("interface.py can't be run in standalone")
