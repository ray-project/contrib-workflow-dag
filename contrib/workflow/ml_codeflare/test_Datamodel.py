import ray
from ray import workflow
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

def feature_union(*inputtuple):
    X_list = []
    y = None
    flag = True
    for feature in list(inputtuple):
        (X, y, flag) = feature
        X_list.append(X)
    X_concat = np.concatenate(X_list, axis=1)
    return (X_concat, y, flag)

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

pipeline = dm.Pipeline('fit_'+datetime.now().strftime("%Y-%m-%d_%H:%M"))
node_a = dm.EstimatorNode('preprocess', preprocessor).get_node()
node_b1 = dm.EstimatorNode('minmaxscaler', MinMaxScaler()).get_node()
node_b2 = dm.EstimatorNode('standardscaler', StandardScaler()).get_node()
node_c = dm.AndNode('feature_union', feature_union).get_node()
node_d = dm.EstimatorNode('decisiontree', DecisionTreeClassifier(max_depth=3)).get_node()

pipeline_input_fit = (X_train, y_train, ExecutionType.FIT)
data_input_fit = {node_a:{0:pipeline_input_fit}}
pipeline.add_edge(node_a, node_b1)
pipeline.add_edge(node_a, node_b2)
pipeline.add_edge(node_b1, node_c)
pipeline.add_edge(node_b2, node_c)
pipeline.add_edge(node_c, node_d)
(X_out, y_out, fit) = pipeline.execute_pipeline(data_input_fit)
print("\n\n output after FIT: ", X_out.shape, y_out.shape, fit)

reactivated_pipeline = dm.Pipeline('predict_'+datetime.now().strftime("%Y-%m-%d_%H:%M"))
pipeline_input_predict = (X_test, y_test, ExecutionType.PREDICT)
data_input_predict = {node_a:{0:pipeline_input_predict}}
reactivated_pipeline.add_edge(node_a, node_b1)
reactivated_pipeline.add_edge(node_a, node_b2)
reactivated_pipeline.add_edge(node_b1, node_c)
reactivated_pipeline.add_edge(node_b2, node_c)
reactivated_pipeline.add_edge(node_c, node_d)
(X_out, y_out, predict) = reactivated_pipeline.execute_pipeline(data_input_predict)
print("\n\n output after PREDICT: ", X_out.shape, y_out.shape, predict)
