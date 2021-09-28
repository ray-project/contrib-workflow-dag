import ray
from ray import workflow
import contrib
import contrib.workflow.graph as contrib_workflow
from contrib.workflow.graph.dag import DAG
import shutil
from enum import Enum

import sys
import inspect

from typing import Callable, List
import numpy as np
import pandas as pd
from pandas import DataFrame
from sklearn import datasets
from sklearn.pipeline import Pipeline
from sklearn.pipeline import FeatureUnion
from sklearn.compose import ColumnTransformer
from sklearn.decomposition import PCA
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
import sklearn.base as base
from sklearn.preprocessing import StandardScaler, MinMaxScaler, MaxAbsScaler, RobustScaler
from sklearn.utils.validation import check_is_fitted
from sklearn.exceptions import NotFittedError
from sklearn.model_selection import train_test_split
from datetime import datetime

storage = "/tmp/ray/workflow_data/"
shutil.rmtree(storage, ignore_errors=True)
#workflow.init(storage)

workflow.init()

class ExecutionType(Enum):
    FIT = 0,
    PREDICT = 1,
    SCORE = 2



@ray.workflow.virtual_actor
class MLNode():
    def __init__(self, estimator):
        super().__init__()
        #self.node_id = node_id
        if estimator is not None:
            self.estimator = estimator

    def fit(self, inputtuple):
        (X, y, mode)= inputtuple
        if base.is_classifier(self.estimator) or base.is_regressor(self.estimator):
            self.estimator.fit(X, y)
            return X, y, mode
        else:
            X = self.estimator.fit_transform(X)
            return X, y, mode

    def predict(self, inputtuple):
        (X, y, mode) = inputtuple
        if base.is_classifier(self.estimator) or base.is_regressor(self.estimator):
            pred_y = self.estimator.predict(X)
            return X, pred_y, mode
        else:
            X = self.estimator.transform(X)
            return X, y, mode

    def score(self, inputtuple):
        (X, y, mode) = inputtuple
        if base.is_classifier(self.estimator) or base.is_regressor(self.estimator):
            score = self.estimator.score(X, y)
            return X, score, mode
        else:
            X = self.estimator.transform(X)
            return X, y, mode

    def get_model(self):
        return self.estimator

    def run_workflow_step(self, inputtuple):
        (X, y, mode) = inputtuple
        if mode == ExecutionType.FIT:
            return self.fit(inputtuple)
        elif mode == ExecutionType.PREDICT:
            return self.predict(inputtuple)
        elif mode == ExecutionType.SCORE:
            return self.score(inputtuple)

    def __getstate__(self):
        return self.estimator

    def __setstate__(self, estimator):
        self.estimator = estimator

def simplenode(inputtuple, handler):
    assert handler is not None
    handler = workflow.get_actor(handler)
    outputtuple = handler.run_workflow_step.run_async(inputtuple)
    return outputtuple

## prepare the data
X = pd.DataFrame(np.random.randint(0,100,size=(10000, 4)), columns=list('ABCD'))
y = pd.DataFrame(np.random.randint(0,2,size=(10000, 1)), columns=['Label'])

numeric_features = X.select_dtypes(include=['int64']).columns
numeric_transformer = Pipeline(steps=[
    ('scaler', StandardScaler())])

## set up preprocessor as StandardScaler
preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features),
        ])

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

node_j = MLNode.get_or_create("node_j", preprocessor)
node_k = MLNode.get_or_create("node_k", MinMaxScaler())
node_l = MLNode.get_or_create("node_l", DecisionTreeClassifier(max_depth=3))

'''
@contrib_workflow.graph.node
def node_jf(inputtuple):
    return simplenode(inputtuple, "node_j")
@contrib_workflow.graph.node
def node_kf(inputtuple):
    return simplenode(inputtuple, "node_k")
@contrib_workflow.graph.node
def node_lf(inputtuple):
    return simplenode(inputtuple, "node_l")
'''

func_dict = {}
func_string = '@contrib.workflow.graph.node\ndef xxxx(inputtuple):\n\treturn simplenode(inputtuple, "yyyy")\nfunc_dict["yyyy"]=xxxx\n'
for nodename in ["node_j", "node_k", "node_l"]:
    declare_func = func_string.replace('xxxx',nodename+'f',2).replace('yyyy',nodename,2)
    exec(declare_func)
print(type(func_dict["node_j"]))

graph = DAG()
pipeline_input_fit = (X_train, y_train, ExecutionType.FIT)
data_input_fit = {node_jf: {0:pipeline_input_fit}}
graph.add_edge(node_jf, node_kf, 0)
graph.add_edge(node_kf, node_lf, 0)
(X_out, y_out, fit) = graph.execute(data_input_fit)
print("\n\n output after FIT: ", X_out.shape, y_out.shape, fit)

graph = DAG()
pipeline_input_predict = (X_test, y_test, ExecutionType.PREDICT)
data_input_predict = {node_jf: {0:pipeline_input_predict}}
graph.add_edge(node_jf, node_kf, 0)
graph.add_edge(node_kf, node_lf, 0)
(X_out, y_out, predict) = graph.execute(data_input_predict)
print("\n\n output after PREDICT: ", X_out.shape, y_out.shape, predict)

nodek_actor = workflow.get_actor("node_k")
scaler = nodek_actor.get_model.run_async()
print(type(ray.get(scaler)))
