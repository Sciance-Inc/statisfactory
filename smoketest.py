#!/bin/python

#  Smoke test the catalog

from statisfactory import Catalog

root = "/home/dev/Documents/10_projets/stratemia/statisfactory/fakerepo"
context = {"PIPELINE": "vdc_P6_S3"}
catalog = Catalog(root, context)

# Load the master dataframe
df = catalog.load("masterFile")

# do complicated statistical trasnformations
df["foo"] = 1

# Save the dataframe to a contextualized Artefact
catalog.save("testDataset", df)


print(df)

print("WOUUUHOUUU")
