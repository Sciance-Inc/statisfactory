#! /usr/bin/python3

# logger.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
Retrieve the package level logger. Configure the handler
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# System packages
import logging
import sys
import os

#############################################################################
#                                 helpers                                   #
#############################################################################


_LOGGERS = dict()


def get_module_logger(mod_name):
    """
    Retrieve the current formatted logger
    """

    try:
        logger = _LOGGERS[mod_name]
    except KeyError:
        logger = logging.getLogger(mod_name)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s [%(name)-12s] %(levelname)-8s %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(os.environ.get("LOG_LEVEL", "DEBUG").upper())
        logger.propagate = False
        _LOGGERS[mod_name] = logger

    return logger


class MixinLogable:
    def __init__(self, *args, **kwargs):
        self._logger = get_module_logger("statisfactory")

    def warn(self, msg):
        self._logger.warn(msg)

    def info(self, msg):
        self._logger.info(msg)

    def debug(self, msg):
        self._logger.debug(msg)


if __name__ == "__main__":
    sys.exit()
