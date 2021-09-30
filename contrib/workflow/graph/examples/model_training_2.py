from ray import workflow
from contrib.workflow import graph
from contrib.workflow.graph import DAG
import numpy as np

import shutil

"""
This example demonstrates how to train, evaluate and predict a model using 
a single graph.

In this example, we train a model to fit y = 2x + 1 by using gradient decent.

This example covers:
1. how to use virtual actor in graph node.
2. how to re-use the same graph to achieve different purposes (train, eval and predict).
"""


storage = "workflow_data"
shutil.rmtree(storage, ignore_errors=True)
workflow.init(storage)


@graph.node
def gen_data(num, noise=True, x_only=False, x=None):
    x = np.array(x) if x else np.random.rand(num)
    if x_only:
        return x
    y = 2 * x + 1
    if noise:
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

    def evaluate(self, eval_data):
        x, y = eval_data
        y_pred = self.predict(x)
        return (np.square(y_pred - y)).mean()

    def execute(self, input_data, execution_type='train'):
        if execution_type == 'train':
            return self.train(input_data)
        if execution_type == 'predict':
            return 'prediction for {} is:  {}'.format(input_data, self.predict(input_data))
        elif execution_type == 'evaluate':
            return 'evaluation error: {}'.format(self.evaluate(input_data))
        else:
            pass


@graph.node
def model_node(input_data, execution_type='train'):
    model = Model.get_or_create("my_model", init_w=0, init_b=0, learning_rate=0.1)
    return model.execute.run_async(input_data, execution_type)


dag = DAG.sequential([gen_data, model_node])

# Train Mode
train_data = {
    gen_data: {
        'num': 100
    }
}
results = []
for _ in range(300):
    results.append(dag.execute(train_data))
for idx, result in enumerate(results):
    print('mini batch: #{}, w: {}, b: {}'.format(idx + 1, *result))


# Eval Mode
eval_data = {
    gen_data: {
        'num': 10,
        'noise': False
    },
    model_node: {
        'execution_type': 'evaluate'
    }
}
print(dag.execute(eval_data))


# Predict Mode
pred_data = {
    gen_data: {
        'num': 10,
        'x_only': True,
        'x': [1, 2, 3, 4]
    },
    model_node: {
        'execution_type': 'predict'
    }
}
print(dag.execute(pred_data))

