# Statisfactory
_A (not yet) Satisfying Statistical Factory_

Primatives for statistical pipelines replication and data centralization (we put the fun out of "statistics").


## Motivation and Scope
Statisfactory is a toolbox to apply and replicate a Statistical pipeline. The purpose of the project is double : 
* Exposing a one-stop-shop with artefacts generates through the analysis by abstracting the location and the retrieval / saving of an artefact away from the Python code ;
* Replicating the same pipeline to multiple inputs, with some input-specific parametrisation done through a yaml based interface.

### Roadmap
* V1 : the V1 is an (exploratory) work to bounce off some ideas about the proper design of such a tool. Spark support, pipelines 's parallelisation, graph inference are (volountary) out of scope. The v1 is not design to cope with big data, but could definetely handles some analysis on a, let'say, scholar dropout project. 
* V2 : add / rework the framework to add the notion of "runner" with a localRunner and  sparkRunner.

## How to contribute.
Don't

## How to use.

### The Catalog object and the fabulous tale of data persistence.

### The Craft object and how did I finally find a way to mess up with the `inspect` package

### Writting a Pipeline for lazzy Statisticians

#### Plumbing pipelines together

## Implementation Details

### On the typed returned API
The v1 uses a dictionnary mapping type. The v2 could be using something more ellegant, but the named returned are not yet supported

```[python]
@Craft.make(catalog)
def identity(masterFile: Artefact, testFile: Artefact) -> Tuple["masterFile":Artefact, "testFile":Artefact]:
    return masterFile, testFile
```

## Hic sunt dracones
* How to inject a spark runner ? 

