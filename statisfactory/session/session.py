#! /usr/bin/python3

# session.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Implements the single entry point to a Statisfactory application
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

import glob
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Mapping, Optional
from warnings import warn
from functools import reduce

import boto3
from dynaconf import Dynaconf, Validator
from lakefs_client import ApiClient, Configuration, models
from lakefs_client.api import repositories_api
from lakefs_client.client import LakeFSClient
from pygit2 import Repository
import tomli

from statisfactory.errors import Errors, Warnings
from statisfactory.IO import Catalog
from statisfactory.logger import MixinLogable, get_module_logger
from statisfactory.operator import Scoped
from statisfactory.loader import (
    get_parameters,
    get_pipelines,
)

#############################################################################
#                                  Script                                   #
#############################################################################


class Session(MixinLogable):
    """
    Represents the single entry point to a Statisfactory application.
    Load the Statisfactory configuration file and executes the registered hook.

    The class exposes :
        * Getter to the ``settings``, parsed during the class instanciation
        * Getters to pipelines definitions and configurations, if any.
        * A decorator : hook_post_init. To help the user registering is own extention.

    The session expose a ``_`` attributes, for the user to register his own datas.

    Implementation details:
    * The class by itself does not do much, but delegates must of the work the the hooks.
    * The hooks can be used to develop plugins : such as an integration to mlflow, the instanciation of a Spark session.
    * The ``_`` attribute schould be used to store user defined custom extension
    """

    _hooks = []

    @staticmethod
    def get_active_session():
        """
        Get the Session, currently on the top of the stack.
        Must be called from a `with` statement
        """

        S = Scoped().get_session()
        if not S:
            raise Errors.E060() from None  # type: ignore

        return S

    def get_path_to_target(self, target: str) -> Path:
        """
        Retrieve the path to "target" file by executing a "fish pass ;)" from the location of the caller
        """

        # Retrieve the "statisfactory.yaml" file
        root = Path("/")
        trg = Path().resolve()
        while True:
            if (trg / target).exists():
                return trg
            trg = trg.parent
            if trg == root:
                raise Errors.E010(target=target)  # type: ignore

    def __init__(self, *, root_folder: str = None):
        """
        Instanciate a Session by searching for the statisfactory.yaml file in the parent folders
        """

        super().__init__(logger_name=__name__)

        # Retrieve the location of the config file
        self._root = Path(root_folder or self.get_path_to_target("pyproject.toml"))

        self.info(f"Initiating Statisfactory to : '{self._root}'")

        # Extract the stati section from the pyproject
        with open(self._root / "pyproject.toml", "rb") as f:
            pyproject_toml = tomli.load(f)
            config = pyproject_toml.get("tool", {}).get("statisfactory", {})

        # Prepare an empty Dynaconf object and inject the config from pyproject
        self._settings = Dynaconf()
        self._settings.update(config)  # type: ignore

        self._settings.validators.register(  # type: ignore
            Validator("configuration", "catalog", must_exist=True),
            Validator("notebook_target", default="jupyter"),
            Validator("project_slug", must_exist=True),
        )  # type: ignore

        # Fire up the validators
        self._settings.validators.validate()  # type: ignore

        # Instanciate the 'user space'
        self._ = {}

        # Instanciate placeholders to be filled by mandatory hooks
        self._catalog: Catalog
        self._pipelines_definitions: Optional[Mapping[str, Any]]
        self._parameters: Optional[Mapping[str, Any]]
        self._aws_session: Optional[boto3.Session] = None
        self._lakefs_client: Optional[LakeFSClient] = None
        self._lakefs_repo: Optional[models.RepositoryCreation] = None
        self._git: Optional[Repository] = None

        # Execute any registered hooks
        for h in Session._hooks:
            h(self)

        self.info("All done ! You are ready to go ! \U00002728 \U0001F370 \U00002728")

    def __enter__(self):
        """
        Push the Session on top of the thread-safe stack
        """

        Scoped.set_session(self)

    def __exit__(self, exception_type, exception_value, exception_traceback):
        """
        Remove the topmost Session of the thread safe stack
        """

        Scoped.set_session(None)

    @property
    def root(self) -> Path:
        """
        Getter for the Root path of the project
        """

        return self._root

    @property
    def settings(self) -> Dynaconf:
        """
        Getter for the session's base setting.
        """

        return self._settings

    @property
    def catalog(self) -> Catalog:
        """
        Getter for the session's catalog.
        """

        return self._catalog

    @property
    def pipelines_definitions(self):
        """
        Getter for the session's pipelines definitions.
        """

        return self._pipelines_definitions

    @property
    def parameters(self):
        """
        Getter for the session's parameters
        """

        return self._parameters

    @property
    def aws_session(self) -> boto3.Session:
        """
        Getter for the AWS client

        Returns:
            boto3.Session: The session configured via the initiaition hook.
        """

        if not self._aws_session:
            raise Errors.E062()  # type: ignore

        return self._aws_session

    @property
    def git(self) -> Repository:
        """
        Getter for the Git repository the session belongs to.
        """

        if not self._git:
            raise Errors.E064()  # type: ignore

        return self._git

    @property
    def lakefs_client(self) -> LakeFSClient:
        """
        Getter for the AWS client
        Returns:
            The lakeFS configured via the initiaition hook.
        """

        if not self._lakefs_client:
            raise Errors.E063()  # type: ignore

        return self._lakefs_client

    @property
    def lakefs_repo(self):
        """
        Return the LakeFS repository's pointer.
        """

        if not self._lakefs_repo:
            raise Errors.E063()  # type: ignore

        return self._lakefs_repo

    @classmethod
    def hook_post_init(cls, last=True) -> Callable:
        """
        Register a `callable_` to be executed after the session instanciation.
        """

        def _(callable_: Callable):

            LOGGER = get_module_logger(__name__)
            LOGGER.debug(f"Registering session's hook : '{callable_.__name__}'")
            if last:
                cls._hooks.append(callable_)
            else:
                cls._hooks.insert(0, callable_)

            return callable_

        return _


class _DefaultHooks:
    """
    Name spaces for the mandatory Session postinits hooks.

    Hooks will be executed int the order they are declared.

    Implementation details:
    * These hooks use the logger from the session, instead of cust
    """

    @staticmethod
    @Session.hook_post_init()
    def set_path_and_pythonpath(sess: Session) -> None:
        """
        Configure the Path to expose the Lib targets.
        """

        if "sources" not in sess.settings:
            return

        src_path = (sess.root / str(sess.settings.sources)).resolve()

        # Insert Lib into the Path
        if src_path not in sys.path:
            sys.path.insert(0, str(src_path))
            sess.info(f"adding '{sess.settings.sources}' to PATH")

        # Create / update the python path
        try:
            os.environ["PYTHONPATH"]
            warn(Warnings.W010)  # type: ignore
        except KeyError:
            os.environ["PYTHONPATH"] = str(src_path)
            sess.info(f"setting PYTHONPATH to '{sess.settings.sources}'")

    @staticmethod
    @Session.hook_post_init()
    def set_settings(sess: Session) -> None:
        """
        Parse the settings file from the conf/ folder and add the settings to the base statisfactory settings
        """

        # Warn the user if a configuration target is missing
        targets = {
            "globals": Warnings.W011,
            "locals": Warnings.W012,
        }

        # fetch config file starting with locals / globals
        base = sess.root / str(sess.settings.configuration)
        types = (base / "*.yml", base / "*.yaml")
        config_files = defaultdict(set)
        for files in types:
            for item in (Path(g) for g in glob.glob(str(files))):
                if item.name.startswith("locals") or item.name.startswith("globals"):
                    config_files[item.stem].add(str(item.resolve()))
                    sess.info(f"Adding '{item.stem}' to catalogs definitions.")

        for target, w in targets.items():
            if not config_files[target]:
                warn(w)

        config_to_loads = list(config_files["globals"]) + list(config_files["locals"])
        # Fetch all the config file, in the reversed preceding order (to allow for variables shadowing)
        settings = Dynaconf(
            validators=[
                Validator("lakefs_bucket", default="s3://lakefs/"),
            ],
            settings_files=config_to_loads,
            load_dotenv=False,
        )

        sess._settings.update(settings)  # type: ignore

    @staticmethod
    @Session.hook_post_init()
    def set_catalog(sess: Session) -> None:
        """
        Attach the catalog to the session
        """

        path = sess.root / str(sess.settings.catalog)
        catalog = Catalog(path=path, session=sess)

        sess._catalog = catalog

    @staticmethod
    @Session.hook_post_init()
    def set_parameters(sess: Session) -> None:
        """
        Parse and attach pipelines configurations to the Session
        """

        if "parameters" not in sess.settings:
            sess.warn("No parameters to set up.")
            return

        path = (sess.root / str(sess.settings.parameters)).resolve()
        parameters = get_parameters(path, sess)

        sess._parameters = parameters

    @staticmethod
    @Session.hook_post_init()
    def set_pipelines_definitions(sess: Session) -> None:
        """
        Parse and attach pipelines to the Session
        """

        if "pipelines_definitions" not in sess.settings:
            sess.warn("No Pipelines definitions to set up.")
            return

        path = (sess.root / str(sess.settings.pipelines_definitions)).resolve()

        pipelines = get_pipelines(path, sess)

        sess._pipelines_definitions = pipelines

    @staticmethod
    @Session.hook_post_init()
    def set_AWS_client(sess: Session) -> None:
        """
        Configure a Mamazon session.

        Args:
            sess (Session): The statisfactory session
        """

        try:
            sess._aws_session = boto3.Session(
                aws_access_key_id=sess.settings["aws_access_key"],
                aws_secret_access_key=sess.settings["aws_secret_access_key"],
                region_name=sess.settings.get(
                    "aws_region", "us-east-1"
                ),  # type: ignore
            )
        except KeyError:
            warn(Warnings.W060)

    @staticmethod
    @Session.hook_post_init()
    def set_git_repo(sess: Session) -> None:
        """
        Configure the Git client.

        Args:
            sess (Session): The statisfactory session updated by the hook
        """

        # Find the Git repository the Session has been started in
        path_to_git = sess.get_path_to_target(".git")
        repo = Repository(path_to_git)
        sess._git = repo

        return

    @staticmethod
    @Session.hook_post_init()
    def set_lakefs_client(sess: Session) -> None:
        """
        Configure a lakefs client.

        Args:
            sess (Session): The statisfactory session updated by the hook
        """

        # Create the configuration
        configuration = Configuration()
        try:
            configuration.username = sess.settings["lakefs_access_key"]
            configuration.password = sess.settings["lakefs_secret_access_key"]
            configuration.host = sess.settings["lakefs_endpoint"]
        except KeyError:
            warn(Warnings.W061)
            return

        # Create a client from the configuration
        sess._lakefs_client = LakeFSClient(configuration)

        # Create the repo using the client
        slug = sess.settings.project_slug
        repo = models.RepositoryCreation(
            name=slug,
            storage_namespace=f"{sess.settings.lakefs_bucket}/{slug}",
            default_branch="main",
        )
        sess._lakefs_repo = repo

        # Check if the repo exists:
        try:
            with ApiClient(configuration) as api_client:
                api_instance = repositories_api.RepositoriesApi(api_client)
                existing = api_instance.list_repositories()
                existing = set([item.id for item in existing["results"]])
                if slug in existing:
                    sess.debug(f"The LakeFS {slug} repo already exists.")
                    return

                # Create the repo
                sess.info(f"Creating the LakeFS repository for {slug}")
                sess._lakefs_client.repositories.create_repository(repo)

        except BaseException as error:
            raise Errors.E065() from error  # type: ignore

        return


#############################################################################
#                                   main                                    #
#############################################################################

if __name__ == "__main__":
    # (*cough*cough) Smoke-test
    raise BaseException("session.py can't run in standalone")
