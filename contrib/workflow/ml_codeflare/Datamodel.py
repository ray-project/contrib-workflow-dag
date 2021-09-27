import ray
from ray import workflow
import contrib
from contrib import workflow as contrib_workflow
from contrib.workflow.node import DataNode
from contrib.workflow.dag import DAG
from contrib.workflow.ml_codeflare.Exceptions import *

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
    
def simplenode(inputtuple, handler):
    assert handler is not None
    handler = workflow.get_actor(handler)
    outputtuple = handler.run_workflow_step.run_async(inputtuple)
    return outputtuple

@ray.workflow.virtual_actor
class PersistedDAG():
    def __init__(self, dag):
        self.__dag = dag
    def set_dag(self, dag):
        self.__dag = dag
    def get_dag(self):
        return self.__dag
    def __getstate__(self):
        return self.__dag
    def __setstate__(self, dag):
        self.__dag = dag

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

class EstimatorNode():
    def __init__(self, node_name: str, estimator: BaseEstimator):
        self.__virtual_actor = MLNode.get_or_create(node_name, estimator)
        self.__node_name = node_name
    def get_node_name(self):
        """
        Returns the node name
        :return: The name of this node
        """
        return self.__node_name
    def get_estimator(self):
        """
        Return the estimator of the node
        :return: The node's estimator
        """
        node_actor = workflow.get_actor(self.__node_name)
        return node_actor.get_model.run_async()
    def get_node(self):
        func_dict = {}
        func_string = '@contrib.workflow.node\ndef xxxx(inputtuple):\n\treturn simplenode(inputtuple, "yyyy")\nfunc_dict["yyyy"]=xxxx\n'
        declare_func = func_string.replace('xxxx',self.__node_name+'f',2).replace('yyyy',self.__node_name,2)
        exec(declare_func)
        # print(func_dict[self.__node_name])
        return func_dict[self.__node_name]

class Pipeline:
    def __init__(self, id=None):
        self.__dag = DAG()
        self.__fanin = {}
        self.__id = None
        if id is not None:
            self.__id = id
            self.__persisteddag = PersistedDAG.get_or_create(self.__id, self.__dag)
    def __str__(self):
        return self.__id__
    def add_edge(self, from_node, to_node):
        if to_node in self.__fanin.keys():
            self.__fanin[to_node] += 1
        else:
            self.__fanin[to_node] = 0
        self.__dag.add_edge(from_node,to_node,self.__fanin[to_node])
    def create_pipeline_via_dag(self, dag):
        self.__dag = dag
        if self.__id is not None:
            self.__persisteddag = workflow.get_actor(self.__id)
            self.__persisteddag.set_dag.run_async(dag)
    def execute_pipeline(self):
        results = self.__dag.execute()
        if self.__id is not None:
            self.__persisteddag = workflow.get_actor(self.__id)
            self.__persisteddag.set_dag.run_async(self.__dag)
        return results
    def return_pipeline(self):
        if self.__id is not None:
            self.__persisteddag = workflow.get_actor(self.__id)
            return self.__persisteddag.get_dag()
        else:
            raise PipelineException('Current pipeline was not saved')
