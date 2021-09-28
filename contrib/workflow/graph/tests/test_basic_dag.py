from contrib.workflow import graph
from contrib.workflow.graph import DAG, FunctionNode
from ray import workflow

# TODO: apply ray's fixture to tests


@graph.node
def identity(x):
    return x


@graph.node
def minus_5(x):
    return x - 5


@graph.node
def add(a, b):
    return a + b


@graph.node
def minus(a, b):
    return a - b


@graph.node
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


factorial_node = FunctionNode(factorial)


def test_sequential():
    # TODO: should be replaced by fixture
    import shutil
    storage = "workflow_data"
    shutil.rmtree(storage, ignore_errors=True)
    workflow.init(storage)

    payload = {
        minus_5: {
            "x": 10
        }
    }

    assert DAG.sequential([minus_5]).execute(data=payload) == 5
    assert DAG.sequential([minus_5, factorial_node]).execute(data=payload) == 120


def test_dag():
    # TODO: should be replaced by fixture
    import shutil
    storage = "workflow_data"
    shutil.rmtree(storage, ignore_errors=True)
    workflow.init(storage)

    dag = DAG()
    dag.add_node(multiply)
    payload = {
        multiply: {
            0: 10,
            1: 20
        }
    }
    assert dag.execute(payload) == 200

    dag = DAG()
    dag.add_edge(minus, multiply, 0)
    payload = {
        minus: {
            0: 10,
            1: 20
        },
        multiply: {
            1: 30
        }
    }
    assert dag.execute(payload) == -300




