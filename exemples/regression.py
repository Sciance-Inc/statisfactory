#! /usr/bin/python3

# regression.py
#
# Project name: statisfactory
# Author: Hugo Juhel
#
# description:
"""
    Run the linear regression exemple using Statisfactory.
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

import pathlib
import pandas as pd
from statisfactory import Craft, Artefact, Catalog, Volatile
from sklearn.linear_model import LinearRegression
from sklearn import datasets


# The directory containing this file
catalog = Catalog("exemples/dummyRepo")


@Craft.make(catalog)
def build_dataframe(samples: int = 500) -> Artefact("masterFile"):
    """
    Generate a dataframe for a regression of "samples" datapoints.
    "samples" can be overwrited through the craft call or the pipeline context.
    """
    x, y = datasets.make_regression(n_samples=samples, n_features=5, n_informative=3)
    df = pd.DataFrame(x, columns=[str(i) for i in range(0, 5)]).assign(y=y)

    return df


# Optionaly, check the generated dataframe : the craft still accept function paramters as usual.
# out = build_dataframe(10)  # Override the samples parameters
# print(out.get("masterFile"))


@Craft.make(catalog)
def train_regression(masterFile: Artefact, fit_intercept=True) -> Volatile("reg"):
    """
    Train a regression on masterfile.
    The craft will propagate non artefact parameters from the pipeline context
    """
    df = masterFile  # Automagiccaly loaded from the filesystem since masterfile is annotated with Artefact

    y = df.y
    x = df[df.columns.difference(["y"])]
    reg = LinearRegression(fit_intercept=fit_intercept).fit(x, y)

    return reg
    # Reg is not defined in the catalog, the object will not be persisted


@Craft.make(catalog)
def save_coeff(reg: Volatile) -> Artefact("coeffs"):
    """
    The function pickles the coefficients of the model.
    The craft can access to the volatile context in which "reg" lives.
    """

    to_pickle = reg.coef_
    return to_pickle


@Craft.make(catalog)
def no_op(coeffs: Artefact):
    """do nothing"""
    pass


df = build_dataframe(samples=500)
print("done")
# Combine the three crafts
p = no_op + build_dataframe + train_regression + save_coeff
print(p)
print("done")
p(samples=10)
print("done")

# print(p)
# p(samples=500)  # Call the pipeline with specific arguments (once)
# p(samples=499, fit_intercept=False)  # Call the pipeline with specific arguments (once)


# Finally use the catalog to control the coeff
c1 = catalog.load("coeffs", samples=499)
c2 = catalog.load("coeffs", samples=500)
# and copy c2 to 0
catalog.save("coeffs", c2, samples=0)

print(c1)
print(c2)

print("done")
