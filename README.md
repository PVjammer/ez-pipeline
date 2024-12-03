# EZ Pipeline
A simple library for creating streaming data pipelines

## Overview
The EZ Pipeline library uses python's multiprocessing library to create multi-stage data processing pipelines where each
stage runs in parallel. The library also supports multithreaded stages for I/O bound tasks (such as API calls). Async and 
batch processing to be added soon. This library uses the `PipelineFunction` primitive which has overloaded __or__ and __ror__
operators to allow pipelines to be created with the pipe (`|`) operator, similar to working with Unix pipes. standard functions
can be added to a pipeline without wrapping them with a `PipelineFunction` by using the pipe (`|`).


## Getting started
Install the libarary
``` bash
pip install .
```

Create a function and wrap is with a PipelineFunction
```python
def _foo(input: Any):
    return do_something(input)

foo = PipelineFunction(_foo)
```

Create a pipeline by chaining your function together with other functions. Each function will be run in order and any
lists provided as input or generated as output from a stage will be flattened so the one item is fed to a stage at a time
(will be madge configurable to allow for batch processing later).
```python
def bar(input: Any):
    return do_something_else(input)

def spam(input: Any):
    return do_a_third_thing(input)


pipeline = foo | bar | spam

input_list = <your data here>

result = pipeline.run(input_list)
```