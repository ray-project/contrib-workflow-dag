from ray import workflow
from contrib.workflow import graph
from contrib.workflow.graph import DAG

import shutil


storage = "workflow_data"
shutil.rmtree(storage, ignore_errors=True)
workflow.init(storage)


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


# example of using positional args
dag = DAG()
dag.add_edge(minus, multiply, 0)
data = {
    minus: {
        0: 10,
        1: 20
    },
    multiply: {
        1: 30
    }
}
result = dag.execute(data)
print(result)

# example of using kwargs
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
result = dag.execute(data)
print(result)
