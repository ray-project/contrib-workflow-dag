
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
import dill
from contrib.workflow.ml_codeflare.WorkflowRouterDirector import *


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

mldeployment = MLPipeline('base_pipeline')
elapsed_time_collection = []
for epochs in range(10):
    for id in range(1):
        p = 'session_id_'+str(id)
        id_plus_data = (p, X_test, y_test, ExecutionType.PREDICT)
        start_time = time()
        (X_out, y_out, predict) = ray.get(mldeployment.remote(id_plus_data))
        print('----------------', p, X_out.shape, y_out.shape,'----------------')
        elapsed_time_collection.append(time()-start_time)

serve.shutdown()
