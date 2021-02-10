# Statisfactory
_A (not yet) Satisfying Statistical Factory_

Primatives for statistical pipelines replication and data centralization (we put the fun out of "statistics").

[[_TOC_]]

- [Statisfactory](#statisfactory)
  * [Motivation and Scope](#motivation-and-scope)
    + [Roadmap](#roadmap)
  * [How to contribute.](#how-to-contribute)
  * [High level presentation](#high-level-presentation)
  * [How to use it](#how-to-use-it)
    + [The not pythonic part : writting the `catalog.yaml`](#the-not-pythonic-part---writting-the--catalogyaml-)
    + [The `Catalog` object and the fabulous tale of data persistence.](#the--catalog--object-and-the-fabulous-tale-of-data-persistence)
    + [The `Craft` object and how did I finally find a way to mess up with the `inspect` package](#the--craft--object-and-how-did-i-finally-find-a-way-to-mess-up-with-the--inspect--package)
    + [Writting a `Pipeline` for lazzy Statisticians](#writting-a--pipeline--for-lazzy-statisticians)
      - ["Same-same but different", contextualizing pipelines](#-same-same-but-different---contextualizing-pipelines)
      - [Plumbing pipelines together](#plumbing-pipelines-together)
      - [~~Fantastics~~ Volatile beasts and where to ~~find~~ persists them.](#--fantastics---volatile-beasts-and-where-to-find-them)
- [tl;dr show-me-the-money](#tl-dr-show-me-the-money)
- [Implementation Details](#implementation-details)
  * [On the typed returned API](#on-the-typed-returned-api)
- [Hic sunt dracones](#hic-sunt-dracones)


## Motivation and Scope
Statisfactory is a toolbox to apply and replicate a Statistical pipeline. The purpose of the project is double : 
* Exposing a one-stop-shop with artefacts generates through the analysis by abstracting the location and the retrieval / saving of an artefact away from the Python code ;
* Replicating the same pipeline to multiple inputs, with some input-specific parametrisation done through a yaml based interface.

### Roadmap
* V1 : the V1 is an (exploratory) work to bounce off some ideas about the proper design of such a tool. Spark support, pipelines 's parallelisation, graph inference are (volountary) out of scope. The v1 is not design to cope with big data, but could definetely handles some analysis on a, let'say, scholar dropout project. 
    * V0.1 focuses on the `Craft` and `Catalog` objects and does not include CLI nor pipelines orchestration (orther than devlopment hooks or bear minmal implementation)
* V2 : add / rework the framework to add the notion of "runner" with a localRunner and  sparkRunner.

## How to contribute.
Don't.

## High level presentation 
* _Statisfactory_ is based on the idea of `Artefacts`. `Artefacts` are something produced or used by a statistical analysis such as a dataset, a report, a trained model ;
* _Statisfactory_ abstracts away the definition of the `Artefact` from it's utilisation and storage. The library uses a map between an `Artefact`'s declaration and it's location called a `catalog` ;
* Any callable that interacts with the `catalog`, by producing or using an `Artefact` can be wraps in a `Craft` ;
* The `Craft` handles the `Artefact` retrieval and storage from the `Catalog` ;
* Multiples `Craft` can be chained together, forming a `Pipeline`. Any `Pipeline` can be personnalized through the use of a `Context`.

## How to use it

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

```
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

```[python]
from statisfactory import Catalog

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = Catalog("/home/me/myProject")

# catalog can then be used to get / save data
df = catalog.load("masterFile")
df['foo'] = 'foo'
catalog.save("masterFile", df)
```

* if required, named arguments can be used as context to interpolate the strings in the artefacts declaration.

```
from statisfactory import Catalog

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = Catalog("/home/me/myProject")

# catalog can then be used to get / save data
df = catalog.load("testDataset", PIPELINE="foobar")
df['foo'] = 'foo'
catalog.save("testDataset", df, PIPELINE="foobar")
```


### The `Craft` object and how did I finally find a way to mess up with the `inspect` package

* The `Craft` is a decorator used to automagically load and saved `Artefacts` from the `catalog`.
* The craft uses:
    * the `Artefact` annotation, to flag the artefacts to load ;
    * the name of the artefact, to flag the artefacts to save.
* The `Craft` must be binded to a `Catalog`

* Use the `Artefact` type hinting to flag an `Artefact` to load:


```[python]
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

* Return dictionnary mapping names to object. Use the `Artefact`'s __name__ to save it

```[python]
from statisfactory import Catalog, Craft, Artefact

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = Catalog("/home/me/myProject")

# Declare and wraps a funtion that UPDATE an artefact
@Craft.make(catalog)
def add_col(masterFile: Artefact):
    masterFile["foo"] = 1
    return {
        "masterFile": masterFile
    }

# Use the function : the craft will fetch the data and saved the outputed masterFile, since the name is the catalog.
add_col()
```

* You can use normal parameter definitions in your function, the `Craft` will defers them to the callable :

```[python]
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

### Writting a `Pipeline` for lazzy Statisticians

* `Pipelines` are built from `Craft`.
* Once defined, a `Pipeline` must be called to be executed.
* One way to declare pipeline, is to __add__ some crafts togethers. Craft are going to be executed in the order they are added to the pipeline. There is no auto-dep builder in the v1.

```[python]
from statisfactory import Catalog, Craft, Artefact

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = Catalog("/home/me/myProject")

# Declare and wraps a funtion that UPDATE an artefact
@Craft.make(catalog)
def add_col(masterFile: Artefact):
    masterFile["foo"] = 1
    return {
        "augmentedFile": masterFile
    }

# Declare and wraps a funtion that uses an artefact
@Craft.make(catalog)
def show_top(augmentedFile: Artefact):
    augmentedFile.head()


# Define the pipeline, with the `add` interface
p = add_col + show_top

# Execute-it :
p()
```

* If you want to add a name to your `Pipeline` or change the parameters, you need to explicitely define the `Pipeline`.

```[python]
from statisfactory import Catalog, Craft, Artefact, Pipeline

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = Catalog("/home/me/myProject")

# Declare and wraps a funtion that UPDATE an artefact
@Craft.make(catalog)
def add_col(masterFile: Artefact):
    masterFile["foo"] = 1
    return {
        "augmentedFile": masterFile
    }

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

```[python]
from statisfactory import Catalog, Craft, Artefact

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = Catalog("/home/me/myProject")

# Declare and wraps a funtion that UPDATE an artefact with a value called val and defaulted to 1
@Craft.make(catalog)
def add_col(masterFile: Artefact, val = 1):
    masterFile["foo"] = val
    return {
        "augmentedFile": masterFile
    }

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

```[python]
p1 = Pipeline() + craft_1
p = craft_2 + craft_3

x = p1 + p # 1 > 2 > 3 
y = p + p1 # 2 > 3 > 1
```

#### ~~Fantastics~~ Volatile beasts and where to ~~find~~ persists them.

* Variables returned by a `Craft` but __not__ declared in the `Catalog` will be added to the context and cascaded to the subsequent `Crafts` requiring them. Warning / error will be raised if keys collide.

```[python]
from statisfactory import Craft, Catalog

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = Catalog("/home/me/myProject")

@Craft.make(catalog)
def create_var():
    """Create a variable called myValue"""
    
    return {
        "myValue": 10,
    }
    
@Craft.make(catalog)
def print_var(myValue):
    """Print the myValue variable"""

    print(myValue)


# Create / Execute the pipeline
(create_var + print_var)()

```

# tl;dr show-me-the-money

* The following example create a pipeline that create a dataframe, run a regression on it and save the coeff of the regression. The `Pipeline` is personnalized by the number of samples to generate. The `catalog.yaml` uses a variable path, to store the differents coeffiencts.
* The pipeline uses the `catalog.yaml` defined in the introduction.

```[python]
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
    return {"coeffs": to_pickle}  # Coeffs is defined in Catalog. The object will be persisted


# Combine the three crafts into a pipeline
p = build_dataframe + train_regression + save_coeff
p(samples=500)  # Call the pipeline with specific arguments (once)
p(samples=100, fit_intercept=False)  # Call the pipeline with specific arguments (once)


# Finally use the catalog to control the coeff
c1 = catalog.load("coeffs", samples=100)
c2 = catalog.load("coeffs", samples=500)

```

# Implementation Details

## On the typed returned API
The v1 uses a dictionnary mapping type. The v2 could be using something more ellegant, but the named returned are not yet supported

```[python]
@Craft.make(catalog)
def identity(masterFile: Artefact, testFile: Artefact) -> Tuple["masterFile":Artefact, "testFile":Artefact]:
    return masterFile, testFile
```

# Hic sunt dracones
* How to inject a spark runner ? 

