import ray
from ray import workflow
import contrib
from contrib import workflow as contrib_workflow
from contrib.workflow.node import DataNode
from contrib.workflow.dag import DAG

from abc import ABC, abstractmethod
from enum import Enum
from sklearn.base import BaseEstimator
import sklearn.base as base
from sklearn.utils.validation import check_is_fitted
from sklearn.exceptions import NotFittedError
from datetime import datetime

class ExecutionType(Enum):
    FIT = 0,
    PREDICT = 1,
    SCORE = 2

@ray.workflow.virtual_actor
class MLNode():
    def __init__(self, estimator):
        self.__createdTime = datetime.now()
        self.__lastInvokedAt = datetime.now()
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

class EstimatorNode():
    def __init__(self, node_name: str, estimator: BaseEstimator):
        self.__virtual_actor = MLNode.get_or_create(node_name, estimator)
        self.__node_name = node_name
    def get_node(self):
        func_dict = {}
        func_string = '@contrib.workflow.node\ndef xxxx(inputtuple):\n\treturn simplenode(inputtuple, "yyyy")\nfunc_dict["yyyy"]=xxxx\n'
        declare_func = func_string.replace('xxxx',self.__node_name+'f',2).replace('yyyy',self.__node_name,2)
        exec(declare_func)
        print(func_dict[self.__node_name])
        return func_dict[self.__node_name]

class Pipeline:
    def __init__(self, id=None):
        if id is not None:
            self.__id = id
        self.__dag = DAG()
        self.__fanin = {}
    def __str__(self):
        return self.__id__
    def add_edge(self, from_node, to_node):
        if to_node in self.__fanin.keys():
            self.__fanin[to_node] += 1
        else:
            self.__fanin[to_node] = 0
        self.__dag.add_edge(from_node,to_node,self.__fanin[to_node])
    def execute_pipeline(self):
        return self.__dag.execute()