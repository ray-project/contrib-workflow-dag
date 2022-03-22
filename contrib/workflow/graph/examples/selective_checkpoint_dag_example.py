import time
import numpy as np
from ray import workflow
from contrib.workflow import graph
from contrib.workflow.graph import DAG

import shutil


storage = "workflow_data"
shutil.rmtree(storage, ignore_errors=True)
workflow.init(storage)


@graph.node
def gen_array(shape):
    np.random.seed(0)
    return np.random.rand(*shape)


@graph.node
def transform(arr):
    return arr * 100


@graph.node
def reduce(arr):
    return np.mean(arr)


# no skip-checkpointing
dag = DAG.sequential([gen_array, transform, reduce])
data = {
    gen_array: {
        0: (500, 500, 500)
    }
}
start = time.time()
print(dag.execute(data))
end = time.time()
print("time cost: ", end-start)


# skip checkpointing
new_gen_array = gen_array.options(checkpoint=False)
new_transform = transform.options(checkpoint=False)
dag = DAG.sequential([new_gen_array, new_transform, reduce])
data = {
    new_gen_array: {
        0: (500, 500, 500)
    }
}
start = time.time()
print(dag.execute(data))
end = time.time()
print("time cost: ", end-start)

