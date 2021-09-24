import ray
from ray import workflow
from contrib import workflow as contrib_workflow
from contrib.workflow.node import DataNode
from contrib.workflow.dag import DAG
import shutil
from enum import Enum

import sys
import inspect

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

storage = "/tmp/ray/workflow_data/"
shutil.rmtree(storage, ignore_errors=True)
#workflow.init(storage)

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

pipeline = dm.Pipeline('run_'+datetime.now().strftime("%Y-%m-%d_%H:%M"))
node_a = dm.EstimatorNode('preprocess', preprocessor).get_node()
node_b = dm.EstimatorNode('minmaxscaler', MinMaxScaler()).get_node()
node_c = dm.EstimatorNode('decisiontree', DecisionTreeClassifier(max_depth=3)).get_node()

pipeline_input_fit = (X_train, y_train, ExecutionType.FIT)
data_input_fit = DataNode("input_fit", pipeline_input_fit)
pipeline.add_edge(data_input_fit, node_a)
pipeline.add_edge(node_a, node_b)
pipeline.add_edge(node_b, node_c)
(X_out, y_out, fit) = pipeline.execute_pipeline()
print("\n\n output after FIT: ", X_out.shape, y_out.shape, fit)
del pipeline

reactivated_pipeline = dm.Pipeline()
pipeline_input_predict = (X_test, y_test, ExecutionType.PREDICT)
data_input_predict = DataNode("input_predict", pipeline_input_predict)
reactivated_pipeline.add_edge(data_input_predict, node_a)
reactivated_pipeline.add_edge(node_a, node_b)
reactivated_pipeline.add_edge(node_b, node_c)
(X_out, y_out, predict) = reactivated_pipeline.execute_pipeline()
print("\n\n output after PREDICT: ", X_out.shape, y_out.shape, predict)
