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


class MixinLogable:
    def __init__(self, logger_name: str = "statisfactory", *args, **kwargs):
        self._logger = get_module_logger(logger_name)

    def warn(self, msg):
        self._logger.warn(msg)

    def info(self, msg):
        self._logger.info(msg)

    def debug(self, msg):
        self._logger.debug(msg)


if __name__ == "__main__":
    sys.exit()
