from ray import workflow
from contrib.workflow import graph
from contrib.workflow.graph import DAG, FunctionNode

import shutil


storage = "workflow_data"
shutil.rmtree(storage, ignore_errors=True)
workflow.init(storage)


@graph.node
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

dag = DAG.sequential([minus_1, factorial_node])
print(dag.execute(data={minus_1: {"x": 5}}))
