# Smoke test the Craft
from statisfactory import Craft, Artefact, Catalog, Pipeline

catalog = Catalog("/home/dev/Documents/10_projets/stratemia/statisfactory/fakerepo")
# Create some craft
@Craft(catalog)
def add_beer(masterFile: Artefact):

    bar = masterFile.copy()
    bar["beer"] = "yes !"

    return {"testDataset": bar}


@Craft()
def print_beer(testDataset: Artefact):
    print(testDataset)


# Create a context in which run the pipline
catalog = Catalog("/home/dev/Documents/10_projets/stratemia/statisfactory/fakerepo")
P = Pipeline("beers dispenser", catalog) + add_beer + print_beer

# Run it with a given context
P(PIPELINE="vdc_P6_S4")
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
