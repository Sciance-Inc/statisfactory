from statisfactory.IO import Catalog  # noqa
from statisfactory.models.models import Artifact, Volatile  # Artefact,
from statisfactory.operator import Craft, Pipeline, _Craft  # noqa
from statisfactory.session import Session  # noqa


import warnings

warnings.warn(
    "The use of 'Artefact' is soon to be deprecated in favor of 'Artifact' (look closely ;)"
)


__version__ = "0.5.0"
