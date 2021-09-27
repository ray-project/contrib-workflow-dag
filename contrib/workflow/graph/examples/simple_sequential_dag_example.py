from ray import workflow
from contrib import workflow as contrib_workflow
from contrib.workflow.graph.node import DataNode, FunctionNode

from contrib.workflow.graph.dag import DAG
import shutil


storage = "workflow_data"
shutil.rmtree(storage, ignore_errors=True)
workflow.init(storage)

data_input = DataNode("data_input", 5)


@contrib_workflow.graph.node
def minus_1(x):
    return x - 1


@workflow.step
def mul(a, b):
    return a * b


@workflow.step
def factorial(n):
    if n == 1:
        return 1
    else:
        return mul.step(n, factorial.step(n - 1))


factorial_node = FunctionNode(factorial)

graph = DAG.sequential([data_input, minus_1, factorial_node])
print(graph.execute())
