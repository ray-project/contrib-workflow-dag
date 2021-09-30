from ray import workflow
from contrib.workflow import graph
from contrib.workflow.graph import DAG

import shutil


storage = "workflow_data"
shutil.rmtree(storage, ignore_errors=True)
workflow.init(storage)


@graph.node
def multiply(a: int, b: int) -> int:
    return a * b


@workflow.virtual_actor
class Counter:
    def __init__(self, init_val):
        self._count = 0
        self._val = init_val

    def incr(self, val):
        pre_val = self._val
        self._count += 1
        self._val += val
        return self._count, pre_val, self._val


@graph.node
def counter_node(v):
    counter = Counter.get_or_create("my_counter", 0)
    return counter.incr.run_async(v)


@graph.node
def parse_result(res):
    return "Test: #{}, Previous Val: {}; New Val: {}".format(*res)


dag = DAG.sequential([multiply, counter_node, parse_result])
data = {
    multiply: {
        0: 10,
        1: 20
    }
}

print(dag.execute(data))  # return: Test: #1, Previous Val: 0; New Val: 200
print(dag.execute(data))  # return: Test: #2, Previous Val: 200; New Val: 400
print(dag.execute(data))  # return: Test: #3, Previous Val: 400; New Val: 600
