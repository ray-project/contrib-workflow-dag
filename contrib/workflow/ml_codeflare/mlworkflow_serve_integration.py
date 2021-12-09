
import ray
from ray import workflow
from ray import serve
import contrib.workflow.graph as contrib_workflow
from contrib.workflow.graph.dag import DAG
import shutil
from enum import Enum
import sys
import inspect
from copy import deepcopy

from typing import Callable, List
import numpy as np
import pandas as pd
from pandas import DataFrame
import sklearn
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

import contrib.workflow.ml_codeflare.Datamodel as dm
from contrib.workflow.ml_codeflare.Datamodel import *

from time import time

def feature_union(*inputtuple):
    X_list = []
    y = None
    flag = True
    for feature in list(inputtuple):
        (X, y, flag) = feature
        X_list.append(X)
    X_concat = np.concatenate(X_list, axis=1)
    return (X_concat, y, flag)

ray.init(address="auto", namespace="serve")

def setup_logger(*args):
    ray._private.ray_logging.setup_logger("warning", ray.ray_constants.LOGGER_FORMAT)
ray.worker.global_worker.run_function_on_all_workers(setup_logger)
setup_logger()

serve.start()
workflow.init()

## prepare the data
X = pd.DataFrame(np.random.randint(0,100,size=(10000, 4)), columns=list('ABCD'))
y = pd.DataFrame(np.random.randint(0,2,size=(10000, 1)), columns=['Label'])

numeric_features = X.select_dtypes(include=['int64']).columns
numeric_transformer = sklearn.pipeline.Pipeline(steps=[('scaler', StandardScaler())])

## set up preprocessor as StandardScaler
preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features),
        ])

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

try:
    workflow.delete('base_pipeline')
except ray.workflow.common.WorkflowNotFoundError:
    pass

pipeline = dm.Pipeline('base_pipeline')
#pipeline = dm.Pipeline('fit_'+datetime.now().strftime("%Y-%m-%d_%H:%M"))
node_a = dm.EstimatorNode('preprocess', preprocessor).get_node()
node_d = dm.EstimatorNode('decisiontree', DecisionTreeClassifier(max_depth=3)).get_node()

pipeline.add_edge(node_a, node_d)
# create a fit pipeline from the base_pipeline
pipeline_input_fit = (X_train, y_train, ExecutionType.FIT)
(X_out, y_out, fit) = pipeline.execute_pipeline(pipeline_input_fit)
print("--------------------- Done training a pipeline ----------------------- ", X_out.shape, y_out.shape, fit)
print("--------------------- Deploy Serve endpoints ----------------------- ")

@serve.deployment(num_replicas = 4)
class WorkflowRouterDirector:
    def __init__(self, *args):
        import nest_asyncio
        nest_asyncio.apply()
        workflow.init()

        self.currentSessions = dict()
        self.template = args[0]
        self.pipeline_name = args[0].get_pipeline_name()

    def clone_pipeline(self, oldpipeline, newsessionid):
        suffix = newsessionid
        cloned_pipeline = dm.Pipeline(newsessionid)
        pipeline_actor = workflow.get_actor(oldpipeline)
        ray.get(pipeline_actor.ready())

        tdag = ray.get(pipeline_actor.get_dag.run_async())
        edges = tdag.get_edges()
        for edge in edges:
            from_va_name = edge[0].get_vaname()
            from_node_actor = workflow.get_actor(from_va_name)
            ray.get(from_node_actor.ready())
            from_fitted_est = from_node_actor.__getstate__.run()
            cloned_from_node = dm.EstimatorNode(from_va_name+'_'+suffix, from_fitted_est).get_node()

            to_va_name = edge[1].get_vaname()
            to_node_actor = workflow.get_actor(to_va_name)
            ray.get(to_node_actor.ready())
            to_fitted_est = to_node_actor.__getstate__.run()
            cloned_to_node = dm.EstimatorNode(to_va_name+'_'+suffix, to_fitted_est).get_node()
            cloned_pipeline.add_edge(cloned_from_node, cloned_to_node)

        cloned_ml_pipeline = deepcopy(self.template)
        cloned_ml_pipeline.set_pipeline(newsessionid, cloned_pipeline)
        return cloned_ml_pipeline

    def __call__(self, *args):
        session_id = args[0][0]
        input = args[0][1:]
        print(f"\n{session_id}: is routed to replica: {serve.get_replica_context().replica_tag}\n")
        if session_id not in self.currentSessions.keys():
            print(f"\n\n{session_id}, NOT FOUND within currentSessions\n\n")
            confirmsessionexist = True
            try:
                known_session = workflow.get_actor(session_id)
                ray.get(known_session.ready())
                cloned_ml_pipeline = deepcopy(self.template)
                cloned_ml_pipeline.set_pipeline(session_id, dm.Pipeline(session_id))
                self.currentSessions[session_id] = cloned_ml_pipeline
            except ray.workflow.storage.base.KeyNotFoundError:
                confirmsessionexist = False
                print(f"\n\n{session_id}, NOT FOUND\n\n")
            if confirmsessionexist is False:
                newpipeline = self.clone_pipeline(self.pipeline_name, session_id)
                self.currentSessions[session_id] = newpipeline

        (X_out, y_out, predict) = self.currentSessions[session_id].execute(input)
        return (X_out, y_out, predict)

    def __str__(self):
        return ','.join(self.currentSessions.keys())

def workflowrouter(obj):
    def wrapper(*args, **kwargs):
        stateful = obj(args[0])
        WorkflowRouterDirector.deploy(stateful)
        statefulrouter = WorkflowRouterDirector.get_handle()
        return statefulrouter
    return wrapper

@workflowrouter
class MLPipeline:
    def __init__(self, pipeline_name):
        # import nest_asyncio
        # nest_asyncio.apply()
        # localstorage = "/tmp/ray/workflow_data/"
        # workflow.init(localstorage)
        self.pipeline_name = pipeline_name

    def set_pipeline(self, pipeline_name, pipeline):
        self.pipeline_name = pipeline_name
        self.pipeline = pipeline

    def get_pipeline_name(self):
        return self.pipeline_name

    def execute(self, pipeline_input):
        (X_out, y_out, predict) = self.pipeline.execute_pipeline(pipeline_input)
        return (X_out, y_out, predict)

    def __reset__(self, pipeline_name):
        self.pipeline_name = pipeline_name

    def __str__(self):
        return self.pipeline_name

mldeployment = MLPipeline('base_pipeline')
elapsed_time_collection = []
for epochs in range(10):
    for id in range(100):
        p = 'session_id_'+str(id)
        id_plus_data = (p, X_test, y_test, ExecutionType.PREDICT)
        start_time = time()
        (X_out, y_out, predict) = ray.get(mldeployment.remote(id_plus_data))
        print('----------------', p, X_out.shape, y_out.shape,'----------------')
        elapsed_time_collection.append(time()-start_time)

f = open('/tmp/elapsed.txt', 'w')
heading='10 percentile, 50 percentile, 90 percentile, 99 percentile \n'
f.write(heading)
result=str(np.percentile(elapsed_time_collection,10))+', '+str(np.percentile(elapsed_time_collection,50))+', '+str(np.percentile(elapsed_time_collection,90))+', '+str(np.percentile(elapsed_time_collection,99))
f.write(result)
f.close()
