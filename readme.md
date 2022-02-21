![test workflow](https://github.com/Sciance-Inc/statisfactory/actions/workflows/test.yml/badge.svg) ![doc workflow](https://github.com/Sciance-Inc/statisfactory/actions/workflows/doc-publish.yml/badge.svg)
# Statisfactory
_A (not yet) Satisfying Statistical Factory_

Primatives for statistical pipelines replication and data centralization (we put the fun out of "statistics").

_maintainer_ : hugo juhel : juhel.hugo@stratemia.com

_API documentation_ : https://sciance-inc.github.io/statisfactory/

[[_TOC_]]



## Motivation and Scope
Statisfactory is a toolbox to apply and replicate a Statistical pipeline. The purpose of the project is double : 
* Exposing a one-stop-shop with artifacts generates through the analysis by abstracting the location and the retrieval / saving of an artifact away from the Python code ;
* Replicating the same pipeline to multiple inputs, with some input-specific parametrisation done through a yaml based interface.


## How to contribute.
Keep it classy, boys :
* Use `git flow` and don't directly publish to `master`;
* Rebase/merge before integrating your changes;
* Use proper commit messages and follow `commitizen` commits conventions;
* Use `bump2version` to ... bump the version.
* I like my code formatted the same way I like my coffee : `Black`. Use-it for code formatting before committing.

## High level presentation 
* _Statisfactory_ is based on the idea of `Artifacts`. `Artifacts` are something produced or used by a statistical analysis such as a dataset, a report, a trained model ;
* _Statisfactory_ abstracts away the definition of the `Artifact` from it's utilisation and storage. The library uses a map between an `Artifact`'s declaration and it's location called a `catalog` ;
* Any callable that interacts with the `catalog`, by producing or using an `Artifact` can be wraps in a `Craft` ;
* The `Craft` handles the `Artifact` retrieval and storage from the `Catalog` ;
* Multiples `Craft` can be chained together, forming a `Pipeline`. Any `Pipeline` can be personnalized through the use of a `Context` and some `Volatile` objects.

## How to use it

__tl;dr : a tl;dr section is available at the end of the documentation__

### Repo skeleton

* _Statisfactory_ requires the `catalog.yaml` file and the `Data` directory to be located together, at the top level of the git repo.
    * The `Data` directory contains all the `Artifacts` (ie, data, in a broad sense) (and schould then have been called `Artifacts`... let's called that technical debt)
    * The `catalog.yml` file declare and maps `artifacts` to their location.

* The following tree show an exemple of a directory using _statisfactory_ :

```
.
├── Data
│   ├── 10_raw
│   │   └── foobar.xlsx
│   ├── 20_transformed
│   └── 30_output
│       └── coeff.pkl
├── configuration
│   ├── locals.yml
│   └── globals.yaml
└── catalog.yaml
└── statisfactory.yaml
```

* The files :
    * The `pyproject.statisfactory` is the entrypoint of the applications and contains binding to the other files. The configuration schould be put in the `pyproject.toml` file.
    * Both `locals.yaml` and `globals.yaml` are key-value configuration files :
        * `locals.yaml` is not gitted and overwrite the key defined in `globals`. It can be used to store credentials and user dependant variables 
        * Both files defines key-values that can be statically interpolated in the `catalogs`
    * `catalog.yaml` contains the definition of artifacts : ie, the outputs and ouptus of the analysis. 
        * `catalog` can also be defined using folders and subfolders (all `yml|yaml` files are considered as catalogs file)
        * `catalog` files are rendered with `Jinja2` using variables defined in `locals` and `globals` files.

### Populating files

#### `pyproject.toml`


```toml
[tool.statisfactory]

# The name of the project. ShcoUld ideally be the Git repo name.
# it's advised to use the slug for LakeFS, S3's bucket and LakeFS
project_slug = "exemple"

# The directory containing the globals and locals files
configuration = "conf"

# The path to the Catalog file or to the root of the directory containing catalogs
catalog = "conf/catalog.yaml"

# The directory containing sources and packages to be made available to pipelines and crafts. Every python packages / script added to this folder is made available for import
sources = "Lib"

# The root packages (inside "sources") in / from which the craft parsed definition would be store.
notebook_target = "__compiled__"

# The file or the folders of yamls files containing the pipeline definitions : the pipeles use craft defined in scripts and builded into "sources"
pipelines_definitions = "Pipelines/definitions/pipelines.yaml"

# The file or the folders of yamls files containing the configurations definitions 
parameters = "Pipelines/configurations/pipelines.yaml"
```

#### `globals` and `locals`
* Globals and locals contains everything that can either be used :
    * by the `catalog` files, for the interpolation of the `{{ jinja2 placeholders }}` .
    * by the `Session` object, the very root of the a `Stati` application. 
        * Some `ArtifactInteractors` / `InteractorsBakend` / `SessionHook` such as the __aws s3 backend__ or the __lakefs__ backend use __AWS_SECRET_KEY__ defined in `globals` or `locals` file.
        * This can be used to provide variables to your own Statisfactory extensions.
#### `catalog` files
_Connectors are going to be completely reworked to be merged into a new SQLArtifact_

* The `Catalog` centralize the `Artifacts` definitions

* The `catalog.yaml` accepts a list of artifacts (represented as yaml dictionaries:
    * an __artifact__ must minimaly have the followings properties : 
        * the __type__ (csv, excel, odbc)
        * the __name__ of the artiface
    * an __artifact__ can have the followings attributes :
        *  __extra__ : an arbitrary mapping (potentialy validable against a __pydantic__ model) of arguments to dispatch to the class in charge of the __Artifact__ deserialization
        *  __load_options__ and __save_options__ : two arbitraries mappings to be used in the __load__ and __save__ methods of the class in charge of the __Artifact__ deserialization.

* The __artifacts__ exposes the following attributes :
    * __type__ : the defaults type of the connector : can be one of `odbc`, `csv`, `xslx`, `datapane`, `pickle`.
        * `pickle` is used to serialize an object
        * `odbc` is used to execute an sql query against a connector
        * `datapane` is used to save a report build using datapane
    * __ODBC__ connectors expect the following attributes in __Extra__:
      * __connection_string__ the DSN / connection string to use
      * __query__ : the sql query to execute. __Required for `type==odbc`__
    * __datapane, pickle, csv, xlsx__, expect the following attribute in the extra fields:
      * __path__ : the path to the artifact.
        * The backend (__s3__, __local filesyste__, __lakefs__) can be specified through the use of the protocole name.

* The `Artifact`'s __path__ attribute can contains __dynamically interpolated variables__ variables. Such a variable is a placeholder for contextualized values, defined at runtime (as opposed to the ones statically defined into `globals|locals` andi nterpolated at the catalog rendering ). 

* Only `Artifacts` declared in the catalog can be loaded / saved using statisfactory. 

##### `catalog.yaml` exemples
* The following snippet shows various interpolations technics and several uses cases for the extra arguments, specifics to each interactor type.

```yaml
- name: qaiData
  type: odbc
  extra:
    connection_string: DRIVER={ODBC Driver 17 for SQL Server};SERVER=myServer.com;DATABASE=myDatabase;
    query: SELECT TOP 10 * FROM SYSOBJECTS WHERE xtype = 'U';
- name: testDataset
    type: csv
    # PIPELINE is a statically defined value, interpolated at the catalog rendering. The value of Pipeline is pulled from globals|locals by Jinja2
    extra:
        path: '20_transformed/{{ PIPELINE }}/testDataset.csv'
- name: masterFile
  type: xslx
  extra: 
    # s3:// trigger the use of the s3 backend.
    path: s3://{{ base_bucket }}/10_raw/foobar.xlsx 
- name: coeffs
  type: pickle
  # !{samples} is a dynamically interpolated values, while executing a craft / pipeline
  extra:
    path: 30_output/model_!{samples}/coeff.pkl
```
### Analysing data, one `Craft` at a time.

#### The `Session` object
* A `session` is the entry point of a `Statisfactory` application and give the uses access to the `Catalogue` as well as the `pipelines_definitions` and  `parameters`
* When instanciated in a stati-enabled repo, the `Session` will try to find the `statisfactory.yaml` in the current folder and it's parents.

```python
from statisfactory import Session

sess = Session()

# Optionaly, one can tels Stati to use a specific path to a folder containing a statisfactory.yaml file
sess2 = Session(path="foo/bar/")

```
#### The `Catalog` object

* The `Catalog` can be used to load / save `Artifacts`, using the __name__ of the `Artifact`.
* The `Catalog` can be accessed from an active `Session`

```python
from statisfactory import Session

# Initiate a session
sess = Session()

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = sess.catalog

# catalog can then be used to get / save data
df = catalog.load("masterFile")
df['foo'] = 'foo'
catalog.save("masterFile", df)
```

* if required, named arguments can be used as context to interpolate the _dynamic values_ in the artifacts's path declaration.

```python
from statisfactory import Session

# Initiate a session
sess = Session()

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = sess.catalog

# catalog can then be used to get / save data
df = catalog.load("coeffs", samples=10)  # Dynamically interpolating the 'samples' as defined in the catalog
```

### The `Craft` object and how did I finally find a way to mess up with the `inspect` package

* The `Craft` is a decorator used to automagically load and saved `Artifacts` from the `catalog`.
* The craft uses:
    * the `Artifact` annotation (type hinting), to flag the artifacts to load ;
    * the name of the artifact, to flag the artifacts to save.
* The `Craft` must be executed in a `Session` context


_The following snipet shows the deserialization of an artifact_

```python
from statisfactory import Session, Craft, Artifact

# Initiate a Stati session
sess = Session()

# Declare and wraps a funtion that uses an artifact. Use the Artifact annotation to identify an artifact.
@Craft()
def show_df(masterFile: Artifact):
    print(masterFile.shape)

# Execute a Craft in the context of a Session
with sess:
    show_df()
```

* Artifact to serialise are indicated through the Typed signature. Use the `Artifact`'s and `Volatile`'s name to (resp.) persist them or add them to the Session context.

```python
from statisfactory import Session, Craft, Artifact

# Initiate a Stati session
sess = Session()

# Declare and wraps a funtion that UPDATE an artifact
@Craft()
def add_col(masterFile: Artifact) -> Artifact("masterFile"):
    masterFile["foo"] = 1
    
    return masterFile

# Execute a Craft in the context of a Session
with sess:
    add_col()
```

* You can use normal parameter definitions in your function, the `Craft` will defers them to the callable :

```python
from statisfactory import Session, Craft, Artifact

# Initiate a Stati session
sess = Session()

# Declare and wraps a funtion that uses an artifact
@Craft()
def show_top(masterFile: Artifact, top: int=10):
    masterFile.head(top)

# Use the function : with default params
with sess:
    show_top()

# Use the function : overwritting the default param :
with sess:
    show_top(5)
```

* You can return multiples `Artifacts` and `Volatile` with the `tuple` notation.


```python
from statisfactory import Session, Craft, Artifact

# Initiate a Stati session
sess = Session()

# Declare and wraps a funtion that uses an artifact and returns two artifacts
@Craft()
def duplicate(masterFile: Artifact) -> (Artifact("masterFile"), Artifact("DuplicatedMasterFile")):
    
    return masterFile, masterFile

with sess:
    duplicate()
```

### Writting a `Pipeline` for lazzy Statisticians

* `Pipelines` are built by summing `Crafts`.
* Once defined, a `Pipeline` must be called to be executed.
* One way to declare pipeline, is to __add__ some crafts togethers. Crafts are executed by solving dependencies between `Craft`.
* You can call `print` on a pipeline to display a textual representation of the DAG (with it's execution order)
* You can call the `plot()` method on a pipeline to display the DAG. The `graphviz` and `pygraphviz` must have been installed.

```python
from statisfactory import Session, Craft, Artifact

# Initiate a Stati session
sess = Session()

@Craft()
def add_col(masterFile: Artifact) -> Artifact('augmentedFile'):
    masterFile["foo"] = 1
    
    return masterFile

# Define a craft that required the output of a previous craft
@Craft()
def show_top(augmentedFile: Artifact):
    augmentedFile.head()


# Define the pipeline, with the `add` interface
p = show_top + add_col  

# Execute-it :
with sess:
    p()  # Execute the pipeline with default parameters
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
from statisfactory import Session, Craft, Artifact, Pipeline

# Initiate a Stati session
sess = Session()

# Declare and wraps a funtion that UPDATE an artifact
@Craft()
def add_col(masterFile: Artifact) -> Artifact('augmentedFile'):
    masterFile["foo"] = 1

    return masterFile
    
# Declare and wraps a funtion that uses an artifact
@Craft()
def show_top(augmentedFile: Artifact):
    augmentedFile.head()


# Define the pipeline, with the `Pipeline` interface
p = Pipeline("A pipeline named Désir") + add_col + show_top

# Execute-it :
with sess:
    p()
```

#### "Same-same but different", contextualizing pipelines
* As for the `Crafts`, you can pass options, or interpolating variables for the path interpolation to the `Pipeline` when calling it. The following example shows how you can execute the the same `Pipeline` with differents options.

```python
from statisfactory import Session, Craft, Artifact

# Initiate a Stati session
catalog = Session()

# Declare and wraps a funtion that UPDATE an artifact with a value called val and defaulted to 1
@Craft()
def add_col(masterFile: Artifact, val=1) -> Artifact('augmentedFile'):
    masterFile["foo"] = val

# Declare and wraps a funtion that uses an artifact
@Craft()
def show_top(top, augmentedFile: Artifact):
    augmentedFile.head(top)

# Define the pipeline, with the `add` interface
p = add_col + show_top

# Execute-it :
with sess:
    p(top=10)  # Val defaulted to 1
    p(top=10, val="boum")  # Val set to "boum"
```

#### One does not simply dispatch arguments
* By default, a parameter dispatched while calling a pipeline is dispatched to all `Crafts` in a pipeline. Such a parameter is named a `shared` parameter. Parameters can be namespaced using thhe name of the Craft. 
* Namespaced parameters have an higher precedence over shared ones.


```python
from statisfactory import Session, Craft, Artifact

# Initiate a Stati session
catalog = Session()

# Declare and wraps a funtion that UPDATE an artifact with a value called val and defaulted to 1
@Craft()
def add_col(masterFile: Artifact, val=1) -> Artifact('augmentedFile'):
    masterFile["foo"] = val

# Declare and wraps a funtion that uses an artifact
@Craft()
def show_top(top, augmentedFile: Artifact):
    augmentedFile.head(top)

# Define the pipeline, with the `add` interface
p = add_col + show_top

# Define a namespaced configuration (Note the shared params alongside the namespaced one)
config = {
    "add_col": {"top":10},
    "show_top": {"val":3},
    "shared_params_1": 1 
}

# Execute-it :
with sess:
    p(**config)
```

#### Plumbing pipelines together

* You can merge `Pipelines` togethers using the __+__ operator. The `Crafts` are executed in the order they are added
* Note that the __left__ pipelines is actually mutated.

```python
p1 = Pipeline() + craft_1
p = craft_2 + craft_3

x = p1 + p # 1 > 2 > 3 
y = p + p1 # 2 > 3 > 1
```

#### ~~Fantastics~~ Volatile beasts and where to ~~find~~ persists them.

* Variables returned by a `Craft` but __not__ declared in the `Catalog` will be added to the context and cascaded to the subsequent `Crafts` requiring them. Use the `Volatile` class to indicate the name of the objects to keep. Warning / error will be raised if keys collide.

```python
from statisfactory import Craft, Session, Volatile

# Initiate a catalog to your poject (ie, the folder containing Data and catalog.yaml)
catalog = Session()

@Craft()
def create_var() -> Volatile("myValue"):
    """Create a variable called myValue"""
    
    return 10
    
@Craft()
def print_var(myValue: Volatile):
    """Print the myValue variable"""

    print(myValue)


p = print_var + create_var
with sess:
    out = p()

out["myValue"]

```
# tl;dr show-me-the-money

* The following example create a pipeline that create a dataframe, run a regression on it and save the coeff of the regression. The `Pipeline` is personnalized by the number of samples to generate. The `catalog.yaml` uses a variable path, to store the differents coeffiencts.
* The pipeline uses the following `catalog.yaml`.

```yaml
- name: masterFile
  type: xslx
  extra:
    path: /foobar/raw/!{samples}_masterfile.xlsx 
- name: coeffs
  type: pickle
  extra: 
    path: /foobar/out/coeff_!{samples}.pkl
```

```python
from statisfactory import Craft, Artifact, Session, Pipeline, Volatile
from sklearn.linear_model import LinearRegression
from sklearn import datasets

sess = Session()

@Craft()
def build_dataframe(samples: int = 500) -> Artifact("masterFile"):
    """
    Generate a dataframe for a regression of "samples" datapoints.
    "samples" can be overwrited through the craft call or the pipeline context.
    """
    x, y = datasets.make_regression(n_samples=samples, n_features=5, n_informative=3)
    df = pd.DataFrame(x, columns=[str(i) for i in range(0, 5)]).assign(y=y)

    return df  # Persist the df, since "masterFile" is defined as an Artifact in the signature

@Craft()
def train_regression(masterFile: Artifact, fit_intercept=True) -> Volatile('reg'):
    """
    Train a regression on masterfile.
    The craft will propagate non artifact parameters from the pipeline context
    """
    df = (
        masterFile
    )  # Automagiccaly loaded from the filesystem since masterfile is annotated with Artifact

    y = df.y
    x = df[df.columns.difference(["y"])]
    reg = LinearRegression(fit_intercept=fit_intercept).fit(x, y)

    return reg # Reg is not defined as an Arteface but as a Volatile. the object will not be persisted.


@Craft()
def save_coeff(reg: Volatile) -> Artifact('coeffs'):
    """
    The function pickles the coefficients of the model.
    The craft can access to the volatile context in which "reg" lives.
    """

    to_pickle = reg.coef_

    return to_pickle  # Coeffs is defined as an Artifact. The object will be persisted


# Combine the three crafts into a pipeline
p = save_coeff + build_dataframe + train_regression

# Check that the Dependencies have been fixed.
print(p)

# Graphically checks the fix, graphviz must be installed
p.plot()

with sess:
    # Call the pipeline with specific arguments (once)
    p(samples=500)  
    # Call the pipeline with specific arguments (once)
    p(samples=100, fit_intercept=False) 


# Finally use the catalog to control the coeff
c1 = sess.catalog.load("coeffs", samples=100)
c2 = sess.catalog.load("coeffs", samples=500)

```

### I have special "special needs", _statisfactory_ got you covered.

* You can extend stati by adding : 
    * An `ArtifactInteractor`, to transform to `Bytes` an object (and to transform back some bytes to the object)
    * A `Backend`, to writes / read the `Bytes` to as service (local, s3, lakefs)
    * Registering a `SessionHook`

#### Defining custom Interactors

* You can register you own interactors by creating a class inheritating from `ArtifactInteractor`. Your class must implements the interface defined by `ArtifactInteractor` and must uses a unique name. The following snipet show the creation of a custom `foobarer` interactor. 
* Once defined, you can use the `interactor_name` in the `catalog.yaml` to use your custom interactor.

__Please, refers to the code to see the actual interface to implement__

> The class uses a pydantic nested model to validate the `extra` field. Extra will be populated using the nested model.

```python
from typing import Optional
from statisfactory.IO import ArtifactInteractor
from pydantic.dataclasses import dataclass

print(ArtifactInteractor.interactors()) # Print the 'default' registered interactors


class Foobar(ArtifactInteractor, interactor_name="foobarer"):
    
    @dataclass
    class Extra:
        egg: int
        ham: Optional[int] = 0

    def __init__(self, artifact, *args, session, **kwargs):
        ...

    def load(self, **kwargs) -> Any:
        ...

    def save(self, asset: Any, **kwargs):
        ...


print(ArtifactInteractor.interactors())  # Print all registered interactors
```

#### Defining custom backends

__Please, refers to the code to see the actual interface to implement__

* You can register you own backend by creating a class inheritating from `Backend`. Your class must implements the interface defined by `Backend` and must uses a unique name. The following snipet show the creation of a custom `Backend`. 
* Once defined, you can use the prteifx specified in the `Backend` declaration, in the `catalog.yaml` to use your custom Backend.


```python
from urllib.parse import ParseResult
from statisfactory.IO import Backend

class Foobar(Backend, prefix="print"):
    def __init__(self, session: Session):
        ...

    def put(self, *, payload: bytes, fragment: ParseResult, **kwargs):
        ...

    def get(self, *, fragment: ParseResult, **kwargs) -> bytes:
        ...
```


#### Registering `Session Hooks`

```python
from statisfactory.session import BaseSession
 
@BaseSession.hook_post_init()
def print_and_set_param(sess: BaseSession) -> None:
    """
    Custom hook to add a value from 'globals'
    """

    # At startup, Stati read globals / locals and make them avalaible from the 'settings' attributes
    param = sess.settings['my_custom_variable']

    # Print the param
    print(param)

    # set the param into the "user" space
    sess._['value'] = param

    return sess
```

# Session, Pipelines, and Crafts prototypes.

__The code is shown for `session_factory` but schould work for  `pipeline_factory` and  `craft_factory` as well__

> Prototypes allows the user to inject it's own objects instead of the one of Stati. The main use cases I have thought for is the registration of hooks, system wise. It's kind of like dependency injectio with the dependency being injected into Statisfactory. It's usefull for the CLi...
> For simple use-cases, it's probably better to import a custom session from another file before instanciating the session.

The `statisfactory` section of the `pyproject.toml` accepts the (optional) specification of a custom class to be used instead of the object returned by `Session`. The intent of this entrypoint is to allow the user to register hooks for all invocations of a Session in the current project. Hooks are, otherwise, limited to the files they are defined in (and the modules called by thoose file, once the Session has been defined).

To register, project-wise, hooks you need : 

1. To define a class inheriting from `statisfactory.session.BaseSession`
2. To register your custom class in the `pyproject.toml`

```python
# custom_foler/custom_session.py

from statisfactory.session import BaseSession
 
# Create a custom session 
class MySession(BaseSession):
    custom_value = 1

# Registering a custom hook
@MySession.hook_post_init()
def do_stuff(sess: MySession) -> None:
    """
    Custom hook
    """
    
    # Do complicated stuff.

    return sess
```

```toml
# pyproject.toml

[tool.statisfactory.entrypoints] 
module = 'custom_foler.custom_session'
session_factory = 'MySession'
```

__Every__ call to `Session()` (in the current project) will now returns your own, personal, `MySession`

# Implementation Details

# Hic sunt dracones
