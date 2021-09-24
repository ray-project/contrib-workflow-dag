from contrib import workflow as contrib_workflow
from contrib.workflow.node import DataNode, FunctionNode, StepFunctionNode
from contrib.workflow.dag import DAG
from ray import workflow

# TODO: apply ray's fixture to tests


data_input_1 = DataNode("input1", 10)
data_input_2 = DataNode("input2", 20)
data_input_3 = DataNode("input3", 30)


@contrib_workflow.node
def identity(x):
    return x


@contrib_workflow.node
def minus_5(x):
    return x - 5


@contrib_workflow.node
def add(a, b):
    return a + b


@contrib_workflow.node
def minus(a, b):
    return a - b


@contrib_workflow.node
def multiply(a, b):
    return a * b


@workflow.step
def mul(a, b):
    return a * b


@workflow.step
def factorial(n):
    if n == 1:
        return 1
    else:
        return mul.step(n, factorial.step(n - 1))


factorial_node = StepFunctionNode(factorial)


def test_sequential():
    # TODO: should be replaced by fixture
    import shutil
    storage = "workflow_data"
    shutil.rmtree(storage, ignore_errors=True)
    workflow.init(storage)

    assert DAG.sequential([data_input_1, minus_5]).execute() == 5
    assert DAG.sequential([data_input_1, minus_5, factorial_node]).execute() == 120


def test_dag():
    # TODO: should be replaced by fixture
    import shutil
    storage = "workflow_data"
    shutil.rmtree(storage, ignore_errors=True)
    workflow.init(storage)

    dag = DAG()
    dag.add_edge(data_input_1, multiply, 0)
    dag.add_edge(data_input_2, multiply, 1)
    assert dag.execute() == 200

    dag = DAG()
    dag.add_edge(data_input_1, minus, 0)
    dag.add_edge(data_input_2, minus, 1)
    dag.add_edge(minus, multiply, 0)
    dag.add_edge(data_input_3, multiply, 1)
    assert dag.execute() == -300




