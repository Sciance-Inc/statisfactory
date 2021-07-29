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
import os
import sys

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
            "%(asctime)s [%(name)-12s] : %(levelname)-1s : %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(os.environ.get("LOG_LEVEL", "DEBUG").upper())
        logger.propagate = False
        _LOGGERS[mod_name] = logger

    return logger


if __name__ == "__main__":
    sys.exit()
