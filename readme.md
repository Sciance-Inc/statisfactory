![test workflow](https://github.com/Sciance-Inc/statisfactory/actions/workflows/test.yml/badge.svg)

![doc workflow](https://github.com/Sciance-Inc/statisfactory/actions/workflows/doc-publish.yml/badge.svg)
# Statisfactory
_A (not yet) Satisfying Statistical Factory_

Primatives for statistical pipelines replication and data centralization (we put the fun out of "statistics").

_maintainer_ : hugo juhel : juhel.hugo@stratemia.com

_API documentation_ : https://sciance-inc.github.io/statisfactory/

[[_TOC_]]



## Motivation and Scope
Statisfactory is a toolbox to apply and replicate a Statistical pipeline. The purpose of the project is double : 
* Exposing a one-stop-shop with artefacts generates through the analysis by abstracting the location and the retrieval / saving of an artefact away from the Python code ;
* Replicating the same pipeline to multiple inputs, with some input-specific parametrisation done through a yaml based interface.


## How to contribute.
Keep it classy, boys :
* Use `git flow` and don't directly publish to `master`;
* Rebase/merge before integrating your changes;
* Use proper commit messages and follow `commitizen` commits conventions;
* Use `bump2version` to ... bump the version.
* I like my code formatted the same way I like my coffee : `Black`. Use-it for code formatting before committing.

## High level presentation 
* _Statisfactory_ is based on the idea of `Artefacts`. `Artefacts` are something produced or used by a statistical analysis such as a dataset, a report, a trained model ;
* _Statisfactory_ abstracts away the definition of the `Artefact` from it's utilisation and storage. The library uses a map between an `Artefact`'s declaration and it's location called a `catalog` ;
* Any callable that interacts with the `catalog`, by producing or using an `Artefact` can be wraps in a `Craft` ;
* The `Craft` handles the `Artefact` retrieval and storage from the `Catalog` ;
* Multiples `Craft` can be chained together, forming a `Pipeline`. Any `Pipeline` can be personnalized through the use of a `Context` and some `Volatile` objects.

## How to use it

__WARNING__ The documentation is for the `v0.1.0` version only.

__tl;dr : a tl;dr section is available at the end of the documentation__

* _Statisfactory_ requires the `catalog.yaml` file and the `Data` directory to be located together, at the top level of the git repo.
    * The `Data` directory contains all the `Artefacts` (ie, data, in a broad sense) (and schould then have been called `Artefacts`... let's called that technical debt)
    * The `catalog.yml` file declare and maps `artefacts` to their location.

* The following tree show an exemple of a directory using _statisfactory_ :

.
├── Data
│   ├── 10_raw
│   │   └── foobar.xlsx
│   ├── 20_transformed
│   └── 30_output
│       └── coeff.pkl
└── catalog.yaml

### The not pythonic part : writting the `catalog.yaml`
* The `catalog.yaml` accepts the following objects :
    * __version__: the version of the catalog.
    * __connectors__: a declaration of the `pyodbc` connector to use.
    * __artefacts__: a declaration of an artefact

* all __connectors__ schould map a name to a connection string or a DSN. The DSN must have been configured on the computer.

* The __artefacts__ exposes the following attributes :
    * __type__ : the type of the connector : can be one of `odbc`, `csv`, `xslx`, `datapane`, `pickle`.
        * `pickle` is used to serialize an object
        * `odbc` is used to execute an sql query against a connector
        * `datapane` is used to save a report build using datapane
    * __connector__ : the name of the connector to use, as defined in `connector`. __Required for `type==odbc`__
    * __query__ : the sql query to execute. __Required for `type==odbc`__
    * __path__ : the path to the artefact. __Required for `type in [csv, xslx, datapane, pickle]`__ . __The path must be relative to `Data`__

* The `Artefact`'s __path__ attribute can contains variables. The variables are placeholders for contextualized values.

* Only `Artefacts` declared in the catalog can be loaded / saved using statisfactory. 

* The following `.yaml` shows a `catalog.yaml` sample :

```yaml
version: 0.0.1
connectors:
  - ibesServer:
      connString: DRIVER={ODBC Driver 17 for SQL Server};SERVER=myServer.com;DATABASE=myDatabase;UID=me;PWD=myPassword
  - sqlserver:
      connString: foo:bar@spam.sql:12234
artefacts:
  - qaiData:
      type: odbc
      connector: ibesServer
      query: SELECT TOP 10 * FROM SYSOBJECTS WHERE xtype = 'U';
  - testDataset:
      type: csv
      path: '20_transformed/{PIPELINE}/testDataset.csv'
  - masterFile:
      type: xslx
      path: 10_raw/foobar.xlsx
  - report:
      type: datapane
      path: 30_outputs/reports.html
  - coeffs:
      type: pickle
      path: 30_output/model_{samples}/coeff.pkl
```

### The `Catalog` object and the fabulous tale of data persistence.

* The `Catalog` can be used to load a `catalog.yml` and save, retrieve `Artefacts` from, using the __name__ of the `Artefact`.

```python
from statisfactory import Catalog

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = Catalog("/home/me/myProject")

# catalog can then be used to get / save data
df = catalog.load("masterFile")
df['foo'] = 'foo'
catalog.save("masterFile", df)
```

* if required, named arguments can be used as context to interpolate the strings in the artefacts declaration.

```python
from statisfactory import Catalog

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = Catalog("/home/me/myProject")

# catalog can then be used to get / save data
df = catalog.load("testDataset", PIPELINE="foobar")
df['foo'] = 'foo'
catalog.save("testDataset", df, PIPELINE="foobar")
```

#### I have special "special needs", _statisfactory_ got you covered.

* You can register you own interactors by creating a class inheritating from `ArtefactInteractor`. Your class must implements the interface defined by `ArtefactInteractor` and must uses a unique name. The following snipet show the creation of a custom `foobarer` interactor. 
* Registering artefact's custom fields as well as custom fields validation is unsuported as of `v0.1`, but one could implements it using the `make_dataclass(base=Artefact)` or by properly using metaprogramming.
* Once defined, you can use the `interactor_name` in the `catalog.yaml` to use your custom interactor.

__Warning__ : the class definition must preceds the catalog instancation

```python
from statisfactory import ArtefactInteractor, Catalog

print(ArtefactInteractor.interactors()) # Print the 'default' registered interactors


class Foobar(ArtefactInteractor, interactor_name="foobarer"):
    def __init__(artefact, *args, **kwargs):
        pass

    def load(self):
        print("loaded")

    def save(self, asset):
        print("saved")


print(ArtefactInteractor.interactors())  # Print all registered interactors


# Instanciate the catalog as usal
catalog = Catalog("exemples/dummyRepo")
```

### The `Craft` object and how did I finally find a way to mess up with the `inspect` package

* The `Craft` is a decorator used to automagically load and saved `Artefacts` from the `catalog`.
* The craft uses:
    * the `Artefact` annotation, to flag the artefacts to load ;
    * the name of the artefact, to flag the artefacts to save.
* The `Craft` must be binded to a `Catalog`

* Use the `Artefact` type hinting to flag an `Artefact` to load:


```python
from statisfactory import Catalog, Craft, Artefact

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = Catalog("/home/me/myProject")

# Declare and wraps a funtion that uses an artefact
@Craft.make(catalog)
def show_df(masterFile: Artefact):
    print(masterFile.shape)

# Use the function : the craft will fetch the data
show_df()
```

* Return are indicated through the Typed signature. Use the `Artefact`'s and `Volatile`'s name to (resp.) persist them or add them to the context.

```python
from statisfactory import Catalog, Craft, Artefact

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = Catalog("/home/me/myProject")

# Declare and wraps a funtion that UPDATE an artefact
@Craft.make(catalog)
def add_col(masterFile: Artefact) -> Artefact("masterFile"):
    masterFile["foo"] = 1
    
    return masterFile

# Use the function : the craft will fetch the data and saved the outputed masterFile, since the name is the catalog.
add_col()
```

* You can use normal parameter definitions in your function, the `Craft` will defers them to the callable :

```python
from statisfactory import Catalog, Craft, Artefact

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = Catalog("/home/me/myProject")

# Declare and wraps a funtion that uses an artefact
@Craft.make(catalog)
def show_top(top: int, masterFile: Artefact):
    masterFile.head(top)

# Use the function : with default params
show_top()

# Use the function : overwritting the default param :
show_top(10)
```

* You can return multiples `Artefacts` and `Volatile` with the `tuple` notation.


```python
from statisfactory import Catalog, Craft, Artefact

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = Catalog("/home/me/myProject")

# Declare and wraps a funtion that uses an artefact
@Craft.make(catalog)
def duplicate( masterFile: Artefact) -> (Artefact("masterFile"), Artefact("DuplicatedMasterFile")):
    
    return masterFile, masterFile

# Use the function : with default params
show_top()

# Use the function : overwritting the default param :
show_top(10)
```

### Writting a `Pipeline` for lazzy Statisticians

* `Pipelines` are built from `Craft`.
* Once defined, a `Pipeline` must be called to be executed.
* One way to declare pipeline, is to __add__ some crafts togethers. Crafts are executed by solving dependencies between `Craft` if possible, or in the order theyre are given
* You can call `print` on a pipeline to display a textual representation of the DAG (with it's execution order)
* You can call the `plot()` method on a pipeline to display the DAG. The `graphviz` and `pygraphviz` must have been installed.

```python
from statisfactory import Catalog, Craft, Artefact

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = Catalog("/home/me/myProject")

# Declare and wraps a funtion that UPDATE an artefact
@Craft.make(catalog)
def add_col(masterFile: Artefact) -> Artefact('augmentedFile'):
    masterFile["foo"] = 1
    
    return masterFile
    

# Declare and wraps a funtion that uses an artefact
@Craft.make(catalog)
def show_top(augmentedFile: Artefact):
    augmentedFile.head()


# Define the pipeline, with the `add` interface
p = add_col + show_top

# Execute-it :
p()
```

* You can check the dependencies between `Craft` using `print` or `.plot()`:

```python

p = show_top + add_col

#  Note that dependencies have been corected.
print(p)


#  Note that dependencies have been corected.
p.plot()  # Require graphviz
```




* If you want to add a name to your `Pipeline` or change the parameters, you need to explicitely define the `Pipeline`.

```python
from statisfactory import Catalog, Craft, Artefact, Pipeline

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = Catalog("/home/me/myProject")

# Declare and wraps a funtion that UPDATE an artefact
@Craft.make(catalog)
def add_col(masterFile: Artefact) -> Artefact('augmentedFile'):
    masterFile["foo"] = 1

    return masterFile
    

# Declare and wraps a funtion that uses an artefact
@Craft.make(catalog)
def show_top(augmentedFile: Artefact):
    augmentedFile.head()


# Define the pipeline, with the `Pipeline` interface
p = Pipeline("A pipeline named Désir") + add_col + show_top

# Execute-it :
p()
```

#### "Same-same but different", contextualizing pipelines
* As for the `Crafts`, you can pass options, or interpolating variables for the path interpolation to the `Pipeline` when calling it. The following example shows how you can execute the the same `Pipeline` with differents options.

```python
from statisfactory import Catalog, Craft, Artefact

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = Catalog("/home/me/myProject")

# Declare and wraps a funtion that UPDATE an artefact with a value called val and defaulted to 1
@Craft.make(catalog)
def add_col(masterFile: Artefact, val=1) -> Artefact('augmentedFile'):
    masterFile["foo"] = val

# Declare and wraps a funtion that uses an artefact
@Craft.make(catalog)
def show_top(top, augmentedFile: Artefact):
    augmentedFile.head(top)


# Define the pipeline, with the `add` interface
p = add_col + show_top

# Execute-it :
p(top=10)  # Val defaulted to 1
p(top=10, val="boum")  # Val set to "boum"
```

#### Plumbing pipelines together

* You can merge `Pipelines` togethers using the __+__ operator. The `Crafts` are executed in the order they are added

```python
p1 = Pipeline() + craft_1
p = craft_2 + craft_3

x = p1 + p # 1 > 2 > 3 
y = p + p1 # 2 > 3 > 1
```

#### ~~Fantastics~~ Volatile beasts and where to ~~find~~ persists them.

* Variables returned by a `Craft` but __not__ declared in the `Catalog` will be added to the context and cascaded to the subsequent `Crafts` requiring them. Use the `Volatile` class to indicate the name of the objects to keep. Warning / error will be raised if keys collide.

```python
from statisfactory import Craft, Catalog, Volatile

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = Catalog("/home/me/myProject")

@Craft.make(catalog)
def create_var() -> Volatile("myValue"):
    """Create a variable called myValue"""
    
    return 10
    
@Craft.make(catalog)
def print_var(myValue):
    """Print the myValue variable"""

    print(myValue)


# Create / Execute the pipeline (dependencies have been resolved :))
(print_var + create_var )()

```

# tl;dr show-me-the-money

* The following example create a pipeline that create a dataframe, run a regression on it and save the coeff of the regression. The `Pipeline` is personnalized by the number of samples to generate. The `catalog.yaml` uses a variable path, to store the differents coeffiencts.
* The pipeline uses the `catalog.yaml` defined in the introduction.

```python
from statisfactory import Craft, Artefact, Catalog, Pipeline, Volatile
from sklearn.linear_model import LinearRegression
from sklearn import datasets

catalog = Catalog("/home/dev/Documents/10_projets/stratemia/statisfactory/fakerepo")


@Craft.make(catalog)
def build_dataframe(samples: int = 500) -> Artefact("masterFile"):
    """
    Generate a dataframe for a regression of "samples" datapoints.
    "samples" can be overwrited through the craft call or the pipeline context.
    """
    x, y = datasets.make_regression(n_samples=samples, n_features=5, n_informative=3)
    df = pd.DataFrame(x, columns=[str(i) for i in range(0, 5)]).assign(y=y)


    return df  # Persist the df, since "masterFile" is defined as an Artefact in the signature


# Optionaly, check the generated dataframe : the craft still accept function paramters as usual.
# out = build_dataframe(10)  # Override the samples parameters
# print(out.get("masterFile"))


@Craft.make(catalog)
def train_regression(masterFile: Artefact, fit_intercept=True) -> Volatile('reg'):
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

    
    return reg # Reg is not defined as an Arteface but as a Volatile. the object will not be persisted.


@Craft.make(catalog)
def save_coeff(reg) -> Artefact('coeffs'):
    """
    The function pickles the coefficients of the model.
    The craft can access to the volatile context in which "reg" lives.
    """

    to_pickle = reg.coef_

    return to_pickle  # Coeffs is defined as an Artefact. The object will be persisted


# Combine the three crafts into a pipeline
p = save_coeff + build_dataframe + train_regression

# Check that the Dependencies have been fixed.
print(p)

# Graphically checks the fix, graphviz must be installed
p.plot()

# Call the pipeline with specific arguments (once)
p(samples=500)  
# Call the pipeline with specific arguments (once)
p(samples=100, fit_intercept=False) 


# Finally use the catalog to control the coeff
c1 = catalog.load("coeffs", samples=100)
c2 = catalog.load("coeffs", samples=500)

```

# Implementation Details

# Hic sunt dracones
Road to v0.2
* Inject a spark runner ? 
* Custom definition of artefacts fields and custom field validation

