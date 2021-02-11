#! /usr/bin/python3

# main.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Package the library with distutils
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

from setuptools import setup, find_packages
import pathlib

# The directory containing this file
HERE = pathlib.Path(__file__).parent
# The text of the README file
README = (HERE / "readme.md").read_text()


setup(
    setup_requires=["setuptools-git-versioning"],
    name="statisfactory",
    description="Satisfying Statistical Factory",
    long_description=README,
    long_description_content_type="text/markdown",
    author="Hugo Juhel",
    author_email="juhel.hugo@stratemia.com",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
    ],
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "marshmallow==3.10.0",
        "pandas>=1.1.2",
        "filelock==3.0.12",
        "dataclasses==0.6",
        "click==7.0",
        "pyodbc>=4.0.30",
        "typing==3.7.4",
        "datapane>=0.9.2",
        "PyYAML==5.4.1",
    ],
)
