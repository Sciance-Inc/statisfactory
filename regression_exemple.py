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

# Import Mathematical stuffs !
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn import datasets

# Import Statisfactory
from statisfactory import Session, Craft, Artefact, Volatile, Pipeline

sess = Session(root_folder="exemples/dummyRepo/")
# Load a context and get the catalog from it.
catalog = sess.catalog
pipelines = sess.pipelines_definitions
# config = sess.pipelines_configurations

default = pd.DataFrame({"a": [1]})

# Create a craft showcasing the session injection and the artifact default !
@Craft.make()
def foobar(masterFile: Artefact = default, samples=666) -> Volatile("foo"):
    return masterFile


@Craft.make()
def barfoo(foo: Volatile, samples):
    print(samples)


p = Pipeline() + foobar + barfoo
p.plot()
with sess:
    out = p()

p = Pipeline(name="Named Pipeline", namespaced=True) + foobar + barfoo
m = {"foobar": {"samples": 5555}, "barfoo": {"samples": 66656}}

with sess:
    out = p(**m)

print("done")


@Pipeline.hook_pre_run()
def foobar(target):
    """ """

    print("LALALALAL")


@Pipeline.hook_post_run()
def foobar(target):
    """ """

    print("POUMPOUMPOUM")


print("done")

# Create a craft that returns an "abstract" artefact : masterFile, for which location is determined at runtime thanks to the provided context (here, the samples)


@Craft.make()
def build_dataframe(samples: int = 500) -> Artefact("masterFile"):
    """
    Generate a dataframe for a regression of "samples" datapoints.
    "samples" can be overwrited through the craft call or the pipeline context.
    """

    x, y = datasets.make_regression(n_samples=samples, n_features=5, n_informative=3)
    df = pd.DataFrame(x, columns=[str(i) for i in range(0, 5)]).assign(y=y)
    return df


with sess:
    df = build_dataframe()
    print("done")

# Call "as is"... Note that samples, in the catalog definition, is dynamically interpollated to the default value

# Call while overwritting the default parameters. Note that the saved dataframe's name has been interpolated with 10
# _ = build_dataframe(samples=10)

# print("pause")


# Create a craft that depends on an "abstract" artefact and that returns, a Volatile, non persisted Artefact. Volatile artefacts are usefull to transfert data between succesives Craft without necessarily storing them.


@Craft.make()
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


## Call train_regression. The craft depends of "masterFile". Masterfile is an abstract artefact, dynamically interpolated by samples, so samples must be provided to the craft's context.
# reg = train_regression(samples=500)


print("pause")

# Create a craft that depends on a Volatile Artefact ! The craft can't be executed standalone but can be executed in a pipeline


@Craft.make()
def save_coeff(reg: Volatile) -> Artefact("coeffs"):
    """
    The function pickles the coefficients of the model.
    The craft can access to the volatile context in which "reg" lives.
    """
    to_pickle = reg.coef_

    return to_pickle


# Create a craft that depends of an abstract artefact : can be run a in a pipeline by using the default value
@Craft.make()
def count_rows(masterFile: Artefact):

    print(f"{len(masterFile)} rows")


# Combine the 3 crafts in a pipeline. Note that order of execution is solved, even for the volatile
p = save_coeff + build_dataframe + train_regression + count_rows


@Craft.hook_on_error(last=False)
def debug(target, error):
    import pdb

    pdb.set_trace()


# Execute the pipeline, note that samples is defaulted to it's value, allowing train_regression and save_coeff to work.
with sess:
    out = p(samples=199)
