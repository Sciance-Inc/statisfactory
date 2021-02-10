import altair as alt
import datapane as dp
import pandas as pd
from typing import Tuple

# Smoke test the Craft
from statisfactory import Craft, Artefact, Catalog, Pipeline
from sklearn.linear_model import LinearRegression
from sklearn import datasets

catalog = Catalog("/home/dev/Documents/10_projets/stratemia/statisfactory/fakerepo")


@Craft.make(catalog)
def build_dataframe(samples: int = 500):
    """
    Generate a dataframe for a regression of "samples" datapoints.
    "samples" can be overwrited through the craft call or the pipeline context.
    """
    x, y = datasets.make_regression(n_samples=samples, n_features=5, n_informative=3)
    df = pd.DataFrame(x, columns=[str(i) for i in range(0, 5)]).assign(y=y)

    return {
        "masterFile": df
    }  # Persist the df, since "masterFile" is defined in catalog


# Optionaly, check the generated dataframe : the craft still accept function paramters as usual.
# out = build_dataframe(10)  # Override the samples parameters
# print(out.get("masterFile"))


@Craft.make(catalog)
def train_regression(masterFile: Artefact, fit_intercept=True):
    """
    Train a regression on masterfile.
    The craft will propagate non artefact parameters from the pipeline context
    """
    df = (
        masterFile
    )  # Automagiccaly loaded from the filesystem since masterfile is annotated with Artefact

    y = df.y
    x = df[df.columns.difference(["y"])]
    reg = LinearRegression(fit_intercept=fit_intercept).fit(x, y)

    return {
        "reg": reg
    }  # Reg is not defined in the catalog, the object will not be persisted


@Craft.make(catalog)
def save_coeff(reg):
    """
    The function pickles the coefficients of the model.
    The craft can access to the volatile context in which "reg" lives.
    """

    to_pickle = reg.coef_
    return {"coeffs": to_pickle}


# Combine the three crafts
p = build_dataframe + train_regression + save_coeff
p(samples=500)  # Call the pipeline with specific arguments (once)
p(samples=499, fit_intercept=False)  # Call the pipeline with specific arguments (once)


# Finally use the catalog to control the coeff
c1 = catalog.load("coeffs", samples=499)
c2 = catalog.load("coeffs", samples=500)
# and copy c2 to 0
catalog.save("coeffs", c2, samples=0)

print(c1)
print(c2)

print("done")

#
# @Craft.make(catalog)
# def train_regression():
#
# model = LogisticRegression()
#
# return {"toPickle": model}
#
#
# @Craft.make(catalog)
# def load_regression(toPickle: Artefact):
# print(toPickle)
#
#
# p0 = Pipeline("foo") + load_regression
# p = train_regression + p0
# p()
# print("done")
#
#
# @Craft.make(catalog)
# def compute_dummy():
# return {"val": 5}
#
#
# @Craft.make(catalog)
# def print_dummy(val):
# print(val)
#
#
# p = Pipeline("jean_bobby", error_on_overwriting=False) + compute_dummy + print_dummy
# out = p(val=1)
#
# print("done7")

# @Craft.make(catalog)
# def add_item(foo: str, masterFile: Artefact):
#
# masterFile["val"] = foo
#
# return {"testDataset": masterFile}
#
#
# @Craft.make(catalog)
# def show_item(testDataset: Artefact, spam=3):
# print(spam)
#
#
# p0 = Pipeline("first") + add_item
# p1 = Pipeline("second") + show_item
# p0 = p0 + p1
#
# p0(PIPELINE="test_1", foo="yup", spam="nop")
#
# p = Pipeline("smoktest") + add_item + show_item
# p(PIPELINE="test_1", foo="yup", spam="nop")
#
# print("done")

#
# @Craft(catalog)
# def show_object(qaiData: Artefact):
#    print(qaiData)
#
#
# show_object()
# print("done")
#
# Create some craft
# @Craft()
# def add_beer(val, masterFile: Artefact, **kwargs):
#
#    bar = masterFile.copy()
#    bar["beer"] = val
#
#    return {"testDataset": bar}

#
# @Craft()
# def get_data(**kwargs):
#    df = pd.read_csv(
#        "https://query1.finance.yahoo.com/v7/finance/download/GOOG?period2=1585222905&interval=1mo&events=history"
#    )
#
#    return {"googleFinance": df}
#
#
# @Craft()
# def build_report(googleFinance: Artefact, **kwargs):
#    chart = (
#        alt.Chart(googleFinance).encode(x="Date:T", y="Open").mark_line().interactive()
#    )
#
#    r = dp.Report(dp.DataTable(googleFinance), dp.Plot(chart))
#
#    return {"report": r}
#
#
# Create a context in which run the pipline
# catalog = Catalog("/home/dev/Documents/10_projets/stratemia/statisfactory/fakerepo")
# P1 = Pipeline("first pip", catalog) + get_data + build_report
#
# Run it with a given context
# P1()
# P2(PIPELINE="vdc_P3_S1", val=2)
# print("done")
#
#
# Open a catalog on the root of the project
# root = "/home/dev/Documents/10_projets/stratemia/statisfactory/fakerepo"
# context = {"PIPELINE": "vdc_P6_S3"}  # optionnaly, add details for replication
# catalog = Catalog(root, **context)
#
# Wrap the statiscal operation with Craft : use type hintings to flag the data to load
#
#
# @Craft(catalog)
# def add_columns(foo, masterFile: Artefact):
#    """ Add a column to df"""
#
#    # masterfile has been retrieved from catalog
#    bar = masterFile.copy()
#    bar["bar"] = foo  # Add the columns
#
#    # Return a mapping : name, assets. Craft will saved the assets for which the name is in the catalog
#    return {"testDataset": bar, "nonPersisted": "nop"}
#
#
# @Craft(catalog)
# def show_df(testDataset: Artefact):
#    """Show the test dataset
#    """
#
#    print(testDataset)
#
#
# out = add_columns("beer")
# show_df()
#
# print("done")
#
# Load the (concrete artefact, not contextualized) master dataframe
# df = catalog.load("masterFile")
#
# do complicated statistical trasnformations
# df["foo"] = 1
#
# Save the dataframe to a contextualized Artefact
# catalog.save("testDataset", str(df))
#
#
# print(df)
#
# print("WOUUUHOUUU")
