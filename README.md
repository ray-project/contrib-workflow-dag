# contrib-workflow-dag

## Overview

This repo provides a DAG layer implementation for running Ray workflows.

DAG layer provides a higher level abstraction on top of workflow steps,
aiming to provide convenience on workflow construction.

## Comparison

**_TODO: This is placeholder for putting a comparison between
using workflow step function vs. workflow graph.   
Discuss with Yi/Zhe/Alex to find a few good examples
for comparison._**   
e.g. 
For sequential, we have `c.step(b.step(a.step())).run` vs 
`DAG.sequential([a, b, c]).execute()`.

Try also to find a multi-branch graph that could benefit from the convenience
of DAG where workflow.step can be very nested and hard-to-read.

## Design
A ***DAG*** contains a set of _**connected Nodes**_.

A DAG/Graph is typically constructed by
`dag.add_edge(from_node, to_node, in_arg_mapping)`.  
`add_edge` adds both Nodes and the connecting edge to the graph, while
`in_arg_mapping` controls how the data stream flows. For example,
`dag.add_edge(A, B, 0)` means adding "Node A -> Node B" to the graph 
while Node A's output will become the input value for first positional argument of Node B.
`in_arg_mapping` can be either positional (int) or kwargs (str).
See [examples](https://github.com/ray-project/contrib-workflow-dag/blob/main/contrib/workflow/examples/simple_dag_example.py#L47)
for better understanding.

DAG can be executed by running `dag.execute()`, optionally with a target
node: `dag.execute(node)` which will execute a sub-graph that runs until
the given node.  

## Quick Start
![dag-example](source/images/dag-example.png)
Run above workflow using graph APIs:
```python
@graph.node
def minus(left: int, right: int) -> int:
    return left - right


@graph.node
def multiply(a: int, b: int) -> int:
    return a * b

dag = DAG()

dag.add_edge(minus, multiply, "a")

data = {
    minus: {
        "left": 10,
        "right": 20
    },
    multiply: {
        "b": 30
    }
}

dag.execute(data)
```
