# contrib-workflow-dag

## Overview

This repo provides a DAG layer implementation for running Ray workflows.

DAG layer provides a higher level abstraction on top of workflow steps,
aiming to provide convenience on workflow construction.

## Comparison

TODO: discuss with Yi/Zhe/Alex to find a good example
for quick comparison.   
e.g. `c.step(b.step(a.step())).run` vs 
`DAG.sequential([a, b, c]).execute()`.  
Try to find a multi-branch graph that could benefit from the convenience
of DAG where workflow.step can be very nested and hard-to-read.

## Design
A ***DAG*** contains a set of _**connected Nodes**_.

A DAG is typically constructed by
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

## Tutorial
Refer to [examples](https://github.com/ray-project/contrib-workflow-dag/tree/main/contrib/workflow/examples) for now.  
TODO: Work with AnyScale team on formulating a tutorial.


## Open Discussions
1. is DataNode needed? i.e. if input data should be 
wrapped in a Node as well?
Having to wrap inputs into a node seems redundant, but has 
the following benefits:
   1. Complete the graph structure.  
For multi-input graph like
[this](https://github.com/ray-project/contrib-workflow-dag/blob/main/contrib/workflow/examples/simple_dag_example.py#L37), 
having DataNode will help complete the graph structure and make the input
layer clear in terms of how many input streams and how each input streams
are connected to operational nodes. And it avoids running execution
as complex as dag.execute(node, input={nodeA: {a: val1, b: val2, ...}, nodeB:{c: val3, d: val4, ...}, nodeC: ...)
   2. Having DataNode sets an interface for all various types of inputs.  
For example, with a data stream from DB connection, one can simply
subclass DataNode and create a new DBNode which can build the db connection
and deliver data.
   3. DataNode is there is many/most graph-based models, e.g. SPSS Modeler.  

A DataNode can ultimately be used as a placeholder which defines
the intput type, how it is connected to the functional nodes and etc.
   
