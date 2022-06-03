![test workflow](https://github.com/Sciance-Inc/statisfactory/actions/workflows/test.yml/badge.svg) ![doc workflow](https://github.com/Sciance-Inc/statisfactory/actions/workflows/doc-publish.yml/badge.svg)
# Statisfactory
_A (not yet) Satisfying Statistical Factory_

Opinionated primatives for statistical pipelines replication and data centralization.

_maintainer_ : hugo juhel : juhel.hugo@stratemia.com

_API documentation_ : https://sciance-inc.github.io/statisfactory/

[[_TOC_]]



## Motivation and Scope
Statisfactory is a toolbox to apply and replicate a Statistical pipeline. The purpose of the project is double : 
* Exposing a one-stop-shop with artifacts generated through the analysis by abstracting the location and the retrieval / saving of an artifact away from the Python code ;
* Replicating the same pipeline to multiple inputs, with some input-specific parametrisation done through a yaml based interface.


## How to contribute.
Keep it classy, boys :
* Use `git flow` and don't directly publish to `master`;
* Rebase/merge before integrating your changes;
* Use proper commit messages and follow `commitizen` commits conventions;
* I like my code formatted the same way I like my coffee : `Black`. Use-it for code formatting before committing.

## High level presentation 
* _Statisfactory_ is based on the idea of `Artifacts`. `Artifacts` are something produced or used by a statistical analysis such as a dataset, a report, a trained model ;
* _Statisfactory_ abstracts away the definition of the `Artifact` from it's utilisation and storage. The library uses a map between an `Artifact`'s declaration and it's location called a `catalog` ;
* Any callable that interacts with the `catalog`, by producing or using an `Artifact` can be wraped in a `Craft` ;
* The `Craft` handles the `Artifact` retrieval and storage from the `Catalog` ;
* Multiples `Craft` can be chained together, forming a `Pipeline`. Any `Pipeline` can be personalized through `parameters`
* `Pipeline` and `parameters` can be defined in `yaml` files and called through a command-line interface.

## How to use it

__a tl;dr section is available at the end of the documentation__

### Repo organisation
* A _Statisfactory_ repo must be configured through the `pyproject.toml` file of your project.
* _Statisfactory_ requiers the project to be used with git.

* The following tree show an exemple of a directory using _statisfactory_ :

```
.
├── lib
│   └── custom_lib    
├── catalog
│   └── raw_data.yaml
├── configuration
│   ├── locals.yml
│   └── globals.yml
├── scripts
│   ├── 10_explanatory_analysis.ipynb
│   └── 20_modelling.ipynb
├── pipelines
│   ├── definitions.yml
│   └── parameters.yaml
└── pyproject.toml

```

* The files :
    * __pyproject.toml__ : the __statisfactory__ configuration
      * The `pyproject.statisfactory` is the entrypoint of the applications and contains binding to the other files. The configuration schould be put in the `pyproject.toml` file.
    * __configuration__ : `pipelines`, `Craft` , Amazon / lakefs configurationss
      * Both `locals.yaml` and `globals.yaml` are key-value configuration files :
          * `locals.yaml` schould not be gitted and overwrite the key defined in `globals`. It can be used to store credentials and user dependant variables. 
          * `globals.yaml` store shared variables.
          * Both files define key-values that can be statically interpolated in the `catalogs`
    * __catalog__ : the definition of artifacts : ie, the inputs and outputs of the data science process. 
        * Multiple `.yaml` can be defined in the `catalog` directory. Each one will be parsed, interpolated (with `globals` and `locals`) and added to the `Catalog` class.
        * `catalog` files are rendered with `Jinja2` using variables defined in `locals` and `globals` files.
          * So, you can use the power of `Jinaj2` (loop, conditional, etc) to define your `artifacts`.
    * __lib__ : custom libraries
        * Custom libraries can be defined in the `lib` directory.
        * When a statisfactory Session is instanciated, the `lib` directory will be added to the `pythonpath`. Any package / library defined in __lib__ can then be imported in your project.
        * You can also define `Crafts` in the `lib` directory, and reference them in your pipelines definitions.
    * __pipelines__: declaring and configuring pipelines
      * __definitions__ : declaring pipelines
        * A Pipeline definition is basically a list of `Craft` names. The `Pipeline` is rendered by automagically solving dependencies between `Craft` objects.
      * __parameters__ : configuring pipelines
        * A parameter set is an arbitrary mapping of the form _craft name_ : _craft configuration_. 
        * _craft configuration_ are passed down to crafts at pipeline execution time.
    * __scripts__ : the `ipynb` files containing your analysis.
      * Datascientists love `ipynb` files (_Eerk_ ). So `statisfactory` can parse any `ipynb` file and render any found `Craft` (by extracting all `craft` annotated cell) into __lib__, to make them usable from your pipelines.

> All folders / files names are conventional and can be changed through the `statisfactory` section of the `pyproject.toml` file.
### Populating files

#### `pyproject.toml`


```toml
[tool.statisfactory]

# The name of the project. ShcoUld ideally be the Git repo name.
# it's advised to use the slug for LakeFS, S3's bucket and LakeFS
# At stati's Session instanciation, an S3 bucket will be created with this name, as well as a LakeFS project (caveat: only if credentials are provided for respectively s3 and lakefs).
project_slug = "exemple" 

# The directory containing the globals and locals files
configuration = "configuration"

# The path to the Catalog file or to the root of the directory containing catalogs
catalog = "catalogs"

# The directory containing sources and packages to be made available to pipelines and crafts. Every python packages / script added to this folder is made available for import
sources = "lib"

# The root packages (inside "sources") in / from which the craft parsed definition would be store.
notebook_target = "jupyter"

# The file or the folder of yamls files containing the pipeline definitions : the pipelines use crafts defined in scripts and builded into "sources" or crafts directly defined in sources.
pipelines_definitions = "pipelines/definitions/pipelines.yaml"

# The file or the folder of yamls files containing the parameters definitions 
parameters = "pipelines/configurations/pipelines.yaml"
```

#### `globals` and `locals`
* Globals and locals contains everything that can either be used :
    * by the `catalog` files, for the interpolation of the `{{ jinja2 placeholders }}` .
    * by the `Session` object, the very root of the a `Stati` application. 
        * Some `ArtifactInteractors` / `InteractorsBakend` / `SessionHook` such as the __aws s3 backend__ or the __lakefs__ backend use __AWS_SECRET_KEY__ defined in `globals` or `locals` file.
        * This can be used to provide variables to your own Statisfactory extensions.
### `catalog` files

* The `Catalog` centralize the `Artifact` objects definitions
* A `Catalog` files accepts a list of `Artifact` definitions (represented as yaml dictionaries).
* The general syntax for an `Artifact` definition is :
  
```yaml
- name: data # the friendly name of the artifact
  type: csv # the type of the artifact. One of [csv, pickle, binary, datapane, odbc, feather, <your custom artifact>]
  extra: # an artifact's specific configuration.
    foo: bar # a list of key-value pair, specific to the artifact configuration
```

#### Mandatory keys : `name`, `type`, `extra`
* An `Artifact` definition must minimaly have the followings properties : 
    * the __type__ of the artifact (csv, excel, odbc,....)
    * the __name__ of the artifact
    * An __extra__ mapping, use to configure the artifact
      * The extra section is specific to the artifact'type.

#### Optional keys : `load_options` and `save_options`
* The `load_options` and `save_options` are mapping that can be used to configure the artifact's loading and saving.
* I mainly (only) use them :
  * to force type casting for the `odbc` artifact
  * to configure the index for the `pandas` interactor
* `load_options` / `save_options` are dispatched to the `ArtifactInteractor.load` / `ArtifactInteractor.save` methods

#### Deep dive : Extra mapping
* Most of the `Artifact` objects need some specific informations to load / save the `Artifact`. For instance, for the `odbc` artifact, you need to provide the query to be executed against the datbase. 
* The extra mapping is as it goes :

__csv__ / __excel__ / __datapane__ / __pickle__ / __binary__
```yaml
name: foo
type: csv
extra:
    path: fooo # The path the the csv file
```

__odbc__
```yaml
name: foo
type: odbc
extra:
    query: SELECT * FROM FOO
    connection_string: The connection string to use to connect (with crendentials interpolated from globals or locals ;) )
```

#### Deep dive : protocol and backend
> For __path-based__ `Artifact` only

* Some `Artifact` are retrieved from / stored to a files system.
* __Statisfactory__ support three files system, called `Backend` (you can of course create your own backends) :
  * __S3__ is the prefered backend when using an Amazon like data lake (Aws s3, Minio)
  * __lakefs__ is usefull if you already have a __lake-fs__ instance.
  * __local__ is _not really usefull_ as you schouldn't store data on your computer (Waht an opiniated micro-framework)
* One can set the backend for a specific `Artifact` by prefixing `Extra.path` attribute with the name of the backend : e.g :
  * `s3://my-project/foo.csv` tells __Statisfactory__ to store the data on __s3__
    * `AWS_SECRET_KEY` and `AWS_ACCESS_KEY` must be provided through `globals` or `locals` (definitely locals !)
  * `lakefs://my-project/foo.csv` tells __Statisfactory__ to store the data on __lakefs__
    * `LAKEFS_ACCESS_KEY`, `LAKEFS_SECRET_ACCESS_KEY`, `LAKEFS_ENDPOINT` must be provided through `globals` or `locals`, 

* The `extra` mapping is a dataclasse declared as a companion object in the `Artifact` definition. Please, refers to this declaration to know how to configure the `extra` mapping for the `Artifact` you want to use. 

#### Deep dive : automating the automation with `Jinja2` 
> To many `Artifacts` ? Keep calm and rander templates ! 

* Defining a lot of `Artifacts` can be cumbersome (I have beers to drink instead ;( ). 
* __Statisfactory__ natively supports `Jinja2` templates in the `Catalog` files as well as the `Parameters` files (c'est-tu hot un peu ?).
* Jinja2 templates are rendered with the `globals` and `locals` dictionaries (you can add custom logic by customizing the `Session` object).
* So, you can natively use :
  * Simple Jinja2 interpolation : `{{ interpolate_me_please }}` 
  * Jinja2 `for` loops : `{% for artfs in ['data_matrix', 'test_set'] %}`
  * Jinja2 `if` statements : `{% if environment == 'production' %}`

#### Deep dive : defining `Artifact` within a constant
* `Artifact` defined manually or with Jinja, are somehow static. 
* When doing data-science in the wild, you might need to define _variations_ of a given Artifact. For instance, you might want to serialize __n__ multiple logistic regression, with different parameters.It would not be praticable to template __n__ `Artificat` objects through Jinja2. 
* __Statisfactory__ allows you to define `Artifact` _within a constant_, using __dynamic__ interpolation. The interpolation is then done at runtime, using data provided at the `Artifact` loading / saving time.
* By default, only the `Extra.path` and `Extra.query` are interpolable. Once again, feel free to add your own `Artifact` classes with custom interpolations strategies.
* Use the `!{value}` to indicate an interpolable variable.

#### `catalog.yaml` : an exemple
* The following snippet shows various interpolations technics and several uses cases for the extra arguments, specifics to each interactor type.

```yaml
- name: qaiData
  type: odbc
  extra:
    connection_string: DRIVER={ODBC Driver 17 for SQL Server};SERVER=myServer.com;DATABASE={{ datastore.database }};UID={{ datastore.user }};PWD={{ datastore.password }}
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

### I'm done defining my `Artifact`, let's start using them !
Once you have defined your `Artifact`s, you can use them in your python code. To do so, you must first instanciate a `Session` object. The `Session` is in charge of wiring up everything you have been configuring.

In your repo : create a _python_ or an _ipynb_ file and instanciate a `Session` object : 

```python
from statisfactory import Session

sess = Session()
```

Boum ! Your are done !

#### Loading / saving an `Artifact`
* You can now read and save your precious `Artifact` by interacting with the `catalog` property of your `Session`.

* Use the __load__ method to fetch an `Artifact`
```python
df = sess.catalog.load('data_matrix')
```

* Use the __save__ method to store an `Artifact`
```python
sess.catalog.save('data_matrix', df)
```

* You can also use additional data to interpolate the __dynamic__ `Artifact` path / query (you know, the weird `!{client_name}` thing)

```python
df = sess.catalog.load('data_matrix', client_name='jean-michel-client')
sess.catalog.save('data_matrix', df, client_name='jean-michel-client')
```

### Analysing data, one `Craft` at a time.
> Obviously, data science is not about saving data, but more about analysing data. __Stati__ helps you to do this, by providing a set of tools to chain your operations. 

* Manually loading and saving your data is not really usefull.
* Instead of directly interacting with the `catalog` property, you can define `Craft`. A `Craft` is a callable that takes datas and outputs datas (looks promising -_-).
* Ideally, the `Craft` also embed the **logic** underlaying the transformation of the __input__ into the __output__.
  
To define a `Craft`:
* Import the `Craft` and `Artifact` objects from __Statisfactory__ : 
```python
from statisfactory import Craft, Artifact
```
* Decorate your function (aka, the 'logic') with `@Craft`
```python
@Craft()
def add_cols:
...
```
* Use __type hinting__ to indicate the input and the output of your function
```python
@Craft()
def add_cols(data_matrix: Artifact) -> Artifact('dataframe'):
    return data_matrix.assign(col=1)
```

Boum ! You have successfully defined a `Craft` ! 
When called, the `Craft` will automagically pull the `data_matrix` (an `Artifact` defined in the `Catalog`) and store the result of the function under the `dataframe` Artifact.

#### The `Craft` object and how did I finally find a way to mess up with the `inspect` package

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

* Artifact to serialize are indicated through the Typed signature. Use the `Artifact`'s and `Volatile`'s name to (resp.) persist them or add them to the Session context.

```python
from statisfactory import Session, Craft, Artifact

# Initiate a Stati session
sess = Session()

# Declare and wraps a funtion that create a new artifact
@Craft()
def add_col(masterFile: Artifact) -> Artifact("masterFile2"):
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

* You can return multiples `Artifacts` and `Volatile` with the `of` notation.


```python
from statisfactory import Session, Craft, Artifact

# Initiate a Stati session
sess = Session()

# Declare and wraps a funtion that uses an artifact and returns two artifacts
@Craft()
def duplicate(masterFile: Artifact) -> Artifact.of("masterFile", "DuplicatedMasterFile"):
    
    return masterFile, masterFile

with sess:
    duplicate()
```


### Writting a `Pipeline` for lazzy Statisticians

* `Pipelines` are built by summing `Crafts`.
* Once defined, a `Pipeline` must be called to be executed.
* One way to declare pipeline, is to __add__ some crafts togethers. Crafts are executed by solving dependencies (ie output/input dependencies) between `Craft`.
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

#  Note that dependencies have been corrected.
print(p)


#  Note that dependencies have been corrected.
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
* As for the `Craft`, you can pass options, or interpolating variables for the `Extra.path / query` interpolation to the `Pipeline` when calling it. The following example shows how you can execute the same `Pipeline` with differents options.

```python
from statisfactory import Session, Craft, Artifact

# Initiate a Stati session
catalog = Session()

# Declare and wraps a funtion that UPDATE an artifact with a value called val and defaulted to 1
@Craft()
def add_col(masterFile: Artifact, val=1) -> Artifact('augmentedFile'):
    masterFile["foo"] = val
    
    return masterfile

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
* By default, a parameter dispatched while calling a pipeline is dispatched to all `Crafts` in a pipeline. Such a parameter is named a `shared` parameter. Parameters can be namespaced using the name of the Craft. 
* Namespaced parameters have an higher precedence over shared ones.


```python
from statisfactory import Session, Craft, Artifact

# Initiate a Stati session
catalog = Session()

# Declare and wraps a funtion that UPDATE an artifact with a value called val and defaulted to 1
@Craft()
def add_col(masterFile: Artifact, val=1) -> Artifact('augmentedFile'):
    masterFile["foo"] = val

    return masterFile

# Declare and wraps a funtion that uses an artifact
@Craft()
def show_top(top, augmentedFile: Artifact):
    augmentedFile.head(top)

# Define the pipeline, with the `add` interface
p = add_col + show_top

# Define a namespaced configuration (Note the shared param alongside the namespaced ones)
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

* You can merge `Pipelines` togethers using the __+__ operator. Between `Crafts` dependencies will be solved anyway.
* Note that the __left__ pipelines is actually mutated.

```python
p1 = Pipeline() + craft_1
p = craft_2 + craft_3

x = p1 + p # 1 > 2 > 3 
y = p + p1 # 2 > 3 > 1
```


#### ~~Fantastics~~ Volatile beasts and where to ~~find~~ persists them.
> Life is too short to define every single `Artifact` ! You can define 'not-persisted' `Artifacts` (donc pas des artifacts en fait) called ... `Volatile`.

* A `Craft` can return or consume a `Volatile` object. A `Volatile` is a **not-persisted** `Artifact`. By not-persisted, I meean, not define in the `Catalog`.
* `Volatile` behave as their persisted couterpart : they can be used to pass data between `Craft`s, and the dependencies solving mechanism still holds. They also got the `.of` method.

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

### Defining `pipelines` definitions and parameters through a yaml file (with some Jinja)

#### definitions
> Operators schould reference `Craft` either manually defined in `sources.something` or in `notebooks.something` and rendered into `sources.jupyter` by a call to `statisfactory compile`
* Pipelines definitions are `yaml` files, that are loaded and rendered at the `Session` loading.
* The following snippet shows a pipeline definition.

```yaml
fetch_design_matrix:
  operators:
    - jupyter.data_moving.move_artifacts.move_then
    - jupyter.design_matrix.build_design_matrix.concat_aspects
build_modelling_tuples:
  operators:
    - jupyter.modelling_tuple.build_modelling_tuple_droppers.build_modelling_tuple_droppers
    - jupyter.modelling_tuple.build_modelling_tuple_exam.build_modelling_tuple_p6
full_experience: # Combine the two previous pipelines into a single one with solved dependencies between crafts
  operators:
    - fetch_design_matrix
    - build_modelling_tuples
```

#### Configurations
> Configurations are namespaced by the fully qualified name of the craft (package.module.craft)
* Multiple configurations can be defined.
* Configuration schould primary be used to be executed with a `Pipeline`

* The following snippet shows a pipeline configuration.

```yaml
# ----- Base config ----- #
base_predict: # The name of the configuration
  shared_param_1: foo # A parameter shared among all crafts
  shared_param_2: bar # A parameter shared among all crafts
  jupyter.modelisation.prediction.step_predict: # The FQN of the craft
    ... # The parameters of the step_predict craft

custom_predict:
  _from: # A list of configs to inherit from
    - base_predict 
    - foobar 
  MODEL_NAME: foobar # # Another shared parameter
```

Since `configurations` support Jinja2, you can do some ~~really obfuscated pipelines configurations~~ tidy definitions.

```yaml
# prediction specific configuration

# ----- CSS / models selections ----- #
{% set to_render = [] %}
{% for css, models in generation.css.items() %} # generation.css is a dictonary defined in globals.yaml
  {% for model_name, targets in models.items() %}
    {% for target in targets %}
      {% set fqn = css + '_' + model_name + '_' + target %}
      {% if fqn not in generation.exclusions.predict_models %}  # exlusion are defined in globals.yaml
      {{- to_render.append([css, model_name, target, fqn]) or '' -}}
      {% endif %}
    {% endfor %}
  {% endfor %}
{% endfor %}

# ----- Base config ----- #
base_predict:
  jupyter.modelisation.prediction.step_predict: # The configuration to be used for the step_predict's craft
    prediction_pipeline_name: default_prediction_pipline
    prediction_pipeline_configuration:
      jupyter.modelisation.prediction.fetch_models:
        models_descriptors_names:
          - xgboost_descriptor

# ----- Client and  model specific configuration ----- #
# Automagically create new configurations with client's specific details
{% for css, model_name, target, fqn in to_render %}
predict_{{ fqn }}:
  _from:
    - base_predict  # inherit from the base configuration
  # Add some clients specific shared parameters
  CSS: {{ css }}
  MODEL_NAME: {{ model_name }}
  MODEL_TARGET: {{ target }}
{% endfor %}
``` 

### Introspection
* `Pipeline` (and `Craft`), `Parameters`, `globals / locals` can be programatically accessed through the `Session` object.

```python
from statisfactory import Session

sess = Session()

param = sess.parameters # is a getter of all parameters defined

# use pipelines_definitions to manipulate pipelines
pipelines = sess.pipelines_definitions
pipeline = pipelines['my_awsome_pipeline']

sess.settings['parameter_defined_in_global']
```

#### Interacting with the `Session` from inside the `Craft`
> Instanciating a session from inside a `Craft` might result in circular-import related errors and other unexpected behaviors. Please, use `get_session` instead.

* Sometimes, we need to access the `Session` from inside a `Craft` to, let's say :
  * Fetch and launch a `Pipeline`
  * Fetch a constant defined in a parameter.
* To do so, you need to use the special (thread-safe) `Session.get_session()` method.

The following exemple show how one can use the Session to manually launch an `Pipeline`

```python
from statisfactory import Session, Artifact

sess = Session()

@Craft()
def start_training_pipeline(data: Artifact, inner_pipeline_name = 'default'):
    """Start a training pipeline"""

    # Get the outermost session on the stack, and fetch the pipeline definition
    sess = Session.get_session()
    pipeline = sess.pipelines_definitions[inner_pipeline_name]

    # There is no need to wraps the call with a `with` statment as the outer craft will be called in a Session
    pipeline(data=data)

with sess:
    start_training_pipeline()
```

# Interacting with the `Session` from the Command-line
> The CLI is (badly) designed to trigger pipeline execution

From a terminal, in a stati-enable project, you can :
* Interact with the pipelines
```bash
statisfactory pipelines ls # list all pipelines
statisfactory pipelines describe <pipeline s name>  # Get the pipeline definition, and the executions stages
```

* Interact with the parameters / configurations
```bash
statisfactory configurations ls # list all configurations
statisfactory configurations describe <configuration s name>  # Get the configuration
```

* Interact with the artifacts
```bash
statisfactory artifacts ls # list all artifacts
statisfactory artifacts describe <artifact s name>  # Get the artifact detailed
```

* Run a pipeline with, optionally, a configuration :
```bash
statisfactory pipelines run <pipeline s name>
statisfactory pipelines run -c [configuration s name] <pipeline s name>
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

# `Session` prototypes.

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
