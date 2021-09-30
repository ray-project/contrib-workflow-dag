from ray import workflow
from contrib.workflow import graph
from contrib.workflow.graph import DAG
import numpy as np

import shutil

"""
This example demonstrates how to train a model using graph + virtual actor.

In this example, we train a model to fit y = 2x + 1 by using gradient decent.

This example covers:
1. how to use virtual actor in graph node.
2. how to re-use the trained model in a different script.
"""

storage = "workflow_data"
shutil.rmtree(storage, ignore_errors=True)
workflow.init(storage)


@graph.node
def gen_data(num):
    x = np.random.rand(num)
    y = 2 * x + 1
    y += np.random.normal(0, 0.1, num)
    return x, y


@workflow.virtual_actor
class Model:
    def __init__(self, init_w, init_b, learning_rate):
        self.w = init_w
        self.b = init_b
        self.learning_rate = learning_rate

    def predict(self, x):
        return self.w * x + self.b

    def train(self, train_data):
        x, y = train_data
        y_pred = self.predict(x)
        w_gradient = -(2. / len(x)) * np.sum(x * (y - y_pred))
        b_gradient = -(2. / len(x)) * np.sum(y - y_pred)
        self.w -= self.learning_rate * w_gradient
        self.b -= self.learning_rate * b_gradient
        return self.w, self.b


@graph.node
def model_train(train_data):
    model = Model.get_or_create("my_model", init_w=0, init_b=0, learning_rate=0.1)
    return model.train.run_async(train_data)


dag = DAG.sequential([gen_data, model_train])
data = {
    gen_data: {
        'num': 100
    }
}

# Here, we execute the graph multiple times, each time with a mini-batch of data
results = []
for _ in range(300):
    results.append(dag.execute(data))
for idx, result in enumerate(results):
    print('mini batch: #{}, w: {}, b: {}'.format(idx + 1, *result))


# Now, in another script, we can re-use the trained model
# Just make sure workflow.init() uses the same storage in the other script
trained_model = workflow.get_actor("my_model")
print(trained_model.predict.run(np.array([1, 2, 3, 4])))

