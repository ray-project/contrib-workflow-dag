import ray
from ray import workflow
from contrib.workflow import graph
from contrib.workflow.graph import DAG, DataNode

import shutil


storage = "workflow_data"
shutil.rmtree(storage, ignore_errors=True)
workflow.init(storage)

# input with DataNode using object
data_input_1 = DataNode("input1", 10)

# input with DataNode using ObjectRef
data_input_2 = DataNode("input2", ray.put(20))


# input with FunctionNode
@graph.node
def data_input_3():
    return 30


@graph.node
def minus(left: int, right: int) -> int:
    return left - right


@graph.node
def multiply(a: int, b: int) -> int:
    return a * b


"""
We are creating the following DAG:

data_input_1----------↓
                    minus----------↓
data_input_2----------↑            ↓
                                multiply
data_input_3-----------------------↑
"""

dag = DAG()
dag.add_edge(data_input_1, minus, 0)
dag.add_edge(data_input_2, minus, 1)
dag.add_edge(minus, multiply, 0)
dag.add_edge(data_input_3, multiply, 1)

print(dag.execute())

dag = DAG()
dag.add_edge(data_input_1, minus, "left")
dag.add_edge(data_input_2, minus, "right")
dag.add_edge(minus, multiply, "a")
dag.add_edge(data_input_3, multiply, "b")

print(dag.execute())
