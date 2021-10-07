import ray
from ray import workflow
import contrib
import contrib.workflow.graph as contrib_workflow
from contrib.workflow.graph.dag import DAG
from contrib.workflow.ml_codeflare.Exceptions import *

from abc import ABC, abstractmethod
from enum import Enum
from sklearn.base import BaseEstimator
import sklearn.base as base
from sklearn.utils.validation import check_is_fitted
from sklearn.exceptions import NotFittedError
from datetime import datetime
from sklearn.model_selection import ParameterGrid

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
    def __init__(self, estimator: BaseEstimator):
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

    @workflow.virtual_actor.readonly
    def predict(self, inputtuple):
        (X, y, mode) = inputtuple
        if base.is_classifier(self.estimator) or base.is_regressor(self.estimator):
            pred_y = self.estimator.predict(X)
            return X, pred_y, mode
        else:
            X = self.estimator.transform(X)
            return X, y, mode

    @workflow.virtual_actor.readonly
    def score(self, inputtuple):
        (X, y, mode) = inputtuple
        if base.is_classifier(self.estimator) or base.is_regressor(self.estimator):
            score = self.estimator.score(X, y)
            return X, score, mode
        else:
            X = self.estimator.transform(X)
            return X, y, mode
            
    @workflow.virtual_actor.readonly
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

class AndNode():
    def __init__(self, node_name, andfunction):
        self.__node_name = node_name
        self.__andfunction = andfunction
    def get_node_name(self):
        """
        Returns the node name
        :return: The name of this node
        """
        return self.__node_name
    def get_node(self):
        return contrib.workflow.graph.node(self.__andfunction)

class EstimatorNode():
    def __init__(self, node_name: str, estimator: BaseEstimator):
        self.__virtual_actor = MLNode.get_or_create(node_name, estimator)
        ray.get(self.__virtual_actor.ready())
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
        # append '_vf' to Estimator __node_name for the corresponding function name (a dag node_name)
        func_dict = {}
        func_string = '@contrib.workflow.graph.node(name="xxxx")\ndef xxxx(inputtuple):\n\treturn simplenode(inputtuple, "yyyy")\nfunc_dict["yyyy"]=xxxx\n'
        declare_func = func_string.replace('xxxx',self.__node_name+'_vf',3).replace('yyyy',self.__node_name,2)
        exec(declare_func)
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
    def get_name(self):
        return self.__id
    def get_dag(self):
        return self.__dag
    def create_pipeline_via_dag(self, dag):
        self.__dag = dag
        if self.__id is not None:
            self.__persisteddag = workflow.get_actor(self.__id)
            self.__persisteddag.set_dag.run_async(dag)
    def execute_pipeline(self, inputdata):
        results = self.__dag.execute(inputdata)
        if self.__id is not None:
            self.__persisteddag = workflow.get_actor(self.__id)
            self.__persisteddag.set_dag.run_async(self.__dag)
        return results
    def return_pipeline(self):
        if self.__id is not None:
            self.__persisteddag = workflow.get_actor(self.__id)
            return self.__persisteddag.get_dag.run_async()
        else:
            raise PipelineException('Current pipeline was not saved')

class PipelineParam:
    """
    This class captures the pipeline parameters, which can be changed for various forms of exploration.
    It is a fairly simple holder class capturing for each node, the corresponding estimators parameters
    as a dictionary.
    It also provides creating a PipelineParam object from a parameter grid, typically used in
    sklearn.GridSearchCV.
    Examples
    --------
    A simple example to create a pipeline param from a parameter grid.
    .. code-block:: python
        param_grid = {
            'pca__n_components': [5, 15, 30, 45, 64],
            'logistic__C': np.logspace(-4, 4, 4),
        }
        pipeline_param = dm.PipelineParam.from_param_grid(param_grid)
    """
    def __init__(self):
        self.__node_name_param_map__ = {}

    @staticmethod
    def from_param_grid(fit_params: dict):
        """
        A method to create a a pipeline param object from a typical parameter grid with the standard
        sklearn convention of __. For example, `pca__n_components` is a parameter for node name pca
        and the parameter name is n_components. The parameter grid creates a full grid exploration of
        the parameters.
        :param fit_params: Dictionary of parameter name in the sklearn convention to the parameter list
        :return: A pipeline param object
        """
        
        pipeline_param = PipelineParam()
        fit_params_nodes = {}
        for pname, pval in fit_params.items():
            if '__' not in pname:
                raise ValueError(
                    "Pipeline.fit does not accept the {} parameter. "
                    "You can pass parameters to specific steps of your "
                    "pipeline using the stepname__parameter format, e.g. "
                    "`Pipeline.fit(X, y, logisticregression__sample_weight"
                    "=sample_weight)`.".format(pname))
            node_name, param = pname.split('__', 1)
            # append node_name to match the function name returned from EstimatorNode.get_node()
            node_name = node_name+'_vf'
            if node_name not in fit_params_nodes.keys():
                fit_params_nodes[node_name] = {}

            fit_params_nodes[node_name][param] = pval

        # we have the split based on convention, now to create paramter grid for each node
        for node_name, param in fit_params_nodes.items():
            pg = ParameterGrid(param)
            pg_list = list(pg)
            for i in range(len(pg_list)):
                p = pg_list[i]
                curr_node_name = node_name + '__' + str(i)
                pipeline_param.add_param(curr_node_name, p)

        return pipeline_param

    def add_param(self, node_name: str, params: dict):
        """
        Add a parameter to the given node name
        :param node_name: Node name to add parameter to
        :param params: Parameter as a dictionary
        :return: None
        """
        self.__node_name_param_map__[node_name] = params

    def get_param(self, node_name: str):
        """
        Returns the parameter dict for the given node name
        :param node_name: Node name to retrieve parameters for
        :return: Dict of parameters
        """
        return self.__node_name_param_map__[node_name]

    def get_all_params(self):
        """
        Return all the parmaters for the given pipeline param
        :return: A dict from node name to the dictionary of parameters
        """
        return self.__node_name_param_map__
