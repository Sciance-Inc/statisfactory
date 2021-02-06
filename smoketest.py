#!/bin/python

#  Smoke test the catalog

from statisfactory import Catalog

catalog = Catalog("/home/dev/Documents/10_projets/stratemia/statisfactory/fakerepo")

df = catalog.load("masterFile")

print("WOUUUHOUUU")
