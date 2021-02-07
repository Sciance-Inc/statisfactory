# Smoke test the Craft
from statisfactory import Craft, Artefact, Catalog, Pipeline

catalog = Catalog("/home/dev/Documents/10_projets/stratemia/statisfactory/fakerepo")


@Craft(catalog)
def show_object(qaiData: Artefact):
    print(qaiData)


show_object()
print("done")

# Create some craft
@Craft()
def add_beer(val, masterFile: Artefact, **kwargs):

    bar = masterFile.copy()
    bar["beer"] = val

    return {"testDataset": bar}


@Craft()
def print_beer(testDataset: Artefact, **kwargs):
    print(testDataset)


# Create a context in which run the pipline
catalog = Catalog("/home/dev/Documents/10_projets/stratemia/statisfactory/fakerepo")
P1 = Pipeline("first pip", catalog) + add_beer + print_beer

# Run it with a given context
P1(PIPELINE="vdc_P6_S4", val=1)
# P2(PIPELINE="vdc_P3_S1", val=2)
print("done")


# Open a catalog on the root of the project
root = "/home/dev/Documents/10_projets/stratemia/statisfactory/fakerepo"
context = {"PIPELINE": "vdc_P6_S3"}  # optionnaly, add details for replication
catalog = Catalog(root, **context)

# Wrap the statiscal operation with Craft : use type hintings to flag the data to load


@Craft(catalog)
def add_columns(foo, masterFile: Artefact):
    """ Add a column to df"""

    # masterfile has been retrieved from catalog
    bar = masterFile.copy()
    bar["bar"] = foo  # Add the columns

    # Return a mapping : name, assets. Craft will saved the assets for which the name is in the catalog
    return {"testDataset": bar, "nonPersisted": "nop"}


@Craft(catalog)
def show_df(testDataset: Artefact):
    """Show the test dataset
    """

    print(testDataset)


out = add_columns("beer")
show_df()

print("done")
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
