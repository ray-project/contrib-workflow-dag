import ray
from ray import workflow
from contrib import workflow as contrib_workflow
from contrib.workflow.node import DataNode

from contrib.workflow.dag import DAG
import shutil


storage = "workflow_data"
shutil.rmtree(storage, ignore_errors=True)
workflow.init(storage)


data_input_1 = DataNode("input1", 10)

# data_input_1 = DataNode("input1", ray.put(10))

# data_input_2 = DataNode("input1", 20)

data_input_2 = DataNode("input2", ray.put(20))


@contrib_workflow.node
def data_input_3():
    return 30


@contrib_workflow.node
def minus(left: int, right: int) -> int:
    return left - right


@contrib_workflow.node
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

graph = DAG()
graph.add_edge(data_input_1, minus, 0)
graph.add_edge(data_input_2, minus, 1)
graph.add_edge(minus, multiply, 0)
graph.add_edge(data_input_3, multiply, 1)

assert -300 == graph.execute()


graph.reset()
graph.add_edge(data_input_1, minus, "left")
graph.add_edge(data_input_2, minus, "right")
graph.add_edge(minus, multiply, "a")
graph.add_edge(data_input_3, multiply, "b")

assert -300 == graph.execute()
